package local

import (
	"fmt"
	"github.com/ProtoML/ProtoML-persist/persist"
	"github.com/ProtoML/ProtoML-persist/persist/persistparsers"
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/formatadaptor"
	"github.com/ProtoML/ProtoML/logger"
	"github.com/ProtoML/ProtoML/utils/osutils"
	//"github.com/mattbaird/elastigo/api"
	//"github.com/mattbaird/elastigo/core"
	"os"
	"os/exec"
	"path"
	"errors"
	"time"
	"github.com/ProtoML/ProtoML-persist/persist/elastic"
	"github.com/ProtoML/ProtoML/utils"
	"encoding/json"
	"strings"
)

const (
	LOGTAG							= "Persist-Local"
	BASE_STATE_DIRECTORY			= ".ProtoML"
	ELASTIC_DIRECTORY				= "elasticsearch"
	PROTOML_TRANSFORMS_DIRECTORY	= "ProtoML-transforms/transforms"
	DIRECTORY_DEPTH					= 4
	HEX_CHARS_PER_DIRECTORY_LEVEL	= 4
	LUIGI_TASK                      = "ProtoML-persist/local/fiber/TransformTask.py"
	TASK_PARARMS_FILE               = "params"
	TASK_LOG_FILE					= "log"
)
 
// key value storage
func keyPath(key string) string {
	// returns the relative directory of a key
	hashed := osutils.MD5Hash(key)
	directories := make([]string, DIRECTORY_DEPTH)
	for x := 0; x < DIRECTORY_DEPTH; x++ {
		directories[x] = hashed[HEX_CHARS_PER_DIRECTORY_LEVEL*x : HEX_CHARS_PER_DIRECTORY_LEVEL*(x+1)]
	}
	return path.Join(directories...)
}

func keyValuePath(key, filename string) (err string) {
	return path.Join(keyPath(key), filename)
}

//  keys
func DatasetFileKey(dataset types.DatasetFile) string {
	return osutils.MD5Hash(fmt.Sprintf("%v",dataset))
}


func DataKey(dataid string) string {
	return dataid + ".data"
}

func TransformKey(transformid string) string {
	return transformid + ".transform"
}

func InducedTransformKey(itransformid string) string {
	return itransformid + ".itransform"
}

func StateKey(stateid string) string {
	return stateid + ".state"
}

type DataFile struct {
	DataId string
	Path string
}

type TaskInsert struct {
	TaskId string
	TaskName string
	Task *exec.Cmd
}
 
type TaskStatus struct {
	TaskId string
	TaskName string
	MsgChan chan TaskStatusMsg
}

type TaskStatusMsg struct {
	TaskId string
	TaskName string
	Finished bool
	Error string
}

type LocalStorage struct {
	Config           persist.Config
	ElasticProcess   *exec.Cmd
	LuigiProcess     *exec.Cmd
	LuigiTaskInsert  chan TaskInsert
	LuigiTaskStatus  chan TaskStatus
	FormatCollection *formatadaptor.FileFormatCollection
}

// key value storage
func (store *LocalStorage) stateDirectory() string {
	return path.Join(store.Config.LocalPersistStorage.RootDir, BASE_STATE_DIRECTORY)
}

func (store *LocalStorage) getKeyPath(key string) string {
	return path.Join(store.stateDirectory(), keyPath(key))
}

func (store *LocalStorage) getFilePath(key, filename string) string {
	return path.Join(store.stateDirectory(), keyValuePath(key, filename))
}

func (store *LocalStorage) absoluteStoragePath(spath string) string {
	return path.Join(store.stateDirectory(), spath)
}
 
func (store *LocalStorage) Init(config persist.Config) (err error) {
	logger.LogInfo(LOGTAG, "Initilizing Persistance Storage")
	store.Config.LocalPersistStorage = config.LocalPersistStorage
	logger.LogDebug(LOGTAG, "Initial Config: %#v", config)

	if config.FormatCollection == nil {
		err = errors.New("Nil format collection.")
		logger.LogDebug(LOGTAG, fmt.Sprintf("%v", err))
		return
	}

	store.FormatCollection = config.FormatCollection

	// validate data directory
	if !osutils.PathExists(store.Config.LocalPersistStorage.DatasetDirectory) {
		err = errors.New(fmt.Sprintf("Cannot access dataset directory %s",store.Config.LocalPersistStorage.DatasetDirectory))
		return
	}

	// run defaults
	if store.Config.LocalPersistStorage.RootDir == "" {
		store.Config.LocalPersistStorage.RootDir, err = os.Getwd()
		if err != nil {
			return
		}
	}
	if store.Config.LocalPersistStorage.StateDir == "" {
		store.Config.LocalPersistStorage.StateDir = BASE_STATE_DIRECTORY
	}

	// check root and template directories exist
	err = osutils.TouchDir(store.Config.LocalPersistStorage.RootDir)
	if err != nil {
		return
	}

	// touch state directory
	err = osutils.TouchDir(store.stateDirectory())
	if err != nil {
		return
	}

	// touch elasticsearch directory
	err = osutils.TouchDir(store.absoluteStoragePath(ELASTIC_DIRECTORY))
	if err != nil {
		return
	}

	// start ElasticSearch
	logger.LogInfo(LOGTAG, "Launching ElasticSearch")
	elastic_cmd := "elasticsearch"
/*	elastic_port := 9200
	if store.Config.LocalPersistStorage.ElasticPort > 0 {
		elastic_port = store.Config.LocalPersistStorage.ElasticPort
	}*/
	elastic_args := []string{
		"-f",
		fmt.Sprintf("-Des.path.data=\"%s\"", store.absoluteStoragePath(ELASTIC_DIRECTORY)),
		fmt.Sprintf("-Des.network.host=\"%s\"", "127.0.0.1"),
//		fmt.Sprintf("-Des.http.port=%d", elastic_port),
	}
//	api.Port = fmt.Sprintf("%d",elastic_port)
	logger.LogDebug(LOGTAG, "Elasticsearch command: %s %v", elastic_cmd, elastic_args)
	store.ElasticProcess = exec.Command(elastic_cmd, elastic_args...)
	err = store.ElasticProcess.Start()
	if err != nil {
		return
	}
	// wait for ElasticSearch bounce
	time.Sleep(time.Second*10)

	// add default data types
	logger.LogDebug(LOGTAG,"%s",types.DefaultDataTypes)
	err = persist.AddDataTypes(types.DefaultDataTypes)
	if err != nil {
		return
	}

	// add ProtoML transforms
	logger.LogDebug(LOGTAG,"Adding ProtoML-transforms")
	protomlDir, err := utils.ProtoMLDir()
	if err != nil {
		err = errors.New(fmt.Sprintf("%s: %v",err, "Cannot use enviromental variable PROTOMLDIR"))
		return
	}
	transformFiles, err := persist.GetTransformFiles(path.Join(protomlDir,PROTOML_TRANSFORMS_DIRECTORY))
	if len(transformFiles) == 0 {
		logger.LogDebug(LOGTAG, "Could not find any ProtoML-transforms")
	}
	for _, transformFile := range transformFiles {
		_, _, err = store.AddTransformFile(transformFile)
		if err != nil {
			return
		}
	}

	// load input data files
	for _, datasetFile := range config.LocalPersistStorage.InputFiles {	
		// redirect path to dataset directory
		datasetFile.Path = path.Join(store.Config.LocalPersistStorage.DatasetDirectory, datasetFile.Path)
		// validate path exists
		if !osutils.PathExists(datasetFile.Path) {
			err = errors.New(fmt.Sprintf("Cannot find input file %s on path %s", path.Base(datasetFile.Path),datasetFile.Path))
			return
		}
	}
	
	// load input data files
	for _, datasetFile := range config.LocalPersistStorage.InputFiles {
		// redirect path to dataset directory
		datasetFile.Path = path.Join(store.Config.LocalPersistStorage.DatasetDirectory, datasetFile.Path)
		_, err = store.AddDataFile(datasetFile)
		if err != nil {
			return
		}
	}
	
	// spin Luigi and luigi task watcher
	err = store.StartLuigi()
	if err != nil {
		return
	}
	return 
}

func (store *LocalStorage) StartLuigi() (err error) {
	store.LuigiTaskInsert = make(chan TaskInsert)
	store.LuigiTaskStatus = make(chan TaskStatus)
	
	// start ElasticSearch
	logger.LogInfo(LOGTAG, "Launching Luigi")
	luigi_cmd := "luigid"
	luigi_args := []string{}
	logger.LogDebug(LOGTAG, "Luigi command: %s %v", luigi_cmd, luigi_args)
	store.LuigiProcess = exec.Command(luigi_cmd, luigi_args...)
	err = store.LuigiProcess.Start()
	if err != nil {
		return
	}
	return
}

func luigiTaskWatcher(taskInsert chan TaskInsert, taskStatus chan TaskStatus) {
	logtag := "LuigiWatcher"
	tasks := make(map[string]TaskInsert)
	taskStatuses := make(map[string]string)
	defer func() {
		for _,task := range tasks {
			task.Task.Process.Kill()
		}
	}()
	for {
		select {
		case insert, ok := <-taskInsert:
			if !ok {
				return
			}
			if task, ok := tasks[insert.TaskId]; ok {
				logger.LogInfo(logtag,"Killing task %s:%s and replacing it with new task %s:%s", task.TaskName, task.TaskId, insert.TaskName, insert.TaskId)
				task.Task.Process.Kill()
				tasks[insert.TaskId] = insert
			} else {
				logger.LogInfo(logtag,"Adding task %s:%s", insert.TaskName, insert.TaskId)
				tasks[insert.TaskId] = insert
			}
		case status, ok := <-taskStatus:
			if !ok {
				return
			}
			tsm := TaskStatusMsg{
				TaskId: status.TaskId,
				TaskName: status.TaskName,
			}
			if ts, ok := taskStatuses[status.TaskId]; ok {
				if len(ts) > 0 {
					tsm.Finished = true
					tsm.Error = ts
				}
			} else {
				tsm.Finished = false
			}
		default:
			for taskid, task := range tasks {
				ps := task.Task.ProcessState
				if ps != nil {
					if ps.Exited() {
						logger.LogDebug(logtag, "Task %s:%s finished", task.TaskName, task.TaskId)
						err := ""
						if !ps.Success() {
							err = fmt.Sprintf("Task %s:%s failed and returned with process state: %s", task.TaskName, task.TaskId, ps)
						}
						taskStatuses[taskid] = err
						delete(tasks,taskid)
					}
				}
			}
		}
	}
}

func (store *LocalStorage) Close() (err error) {
	logger.LogInfo(LOGTAG,"Closing persistance")
	if store.ElasticProcess != nil {
		err = store.ElasticProcess.Process.Signal(os.Interrupt)
		err = store.ElasticProcess.Wait()
	}
	if store.LuigiProcess != nil {
		err = store.LuigiProcess.Process.Signal(os.Interrupt)
		err = store.LuigiProcess.Wait()
	}
	return
}

func (store *LocalStorage) IsDone(itransformId string) (bool, error) {
	itransform, err := elastic.GetInducedTransform(itransformId)
	if err != nil {
		return false, err
	}
	mchan := make(chan TaskStatusMsg)
	store.LuigiTaskStatus <- TaskStatus{itransformId, itransform.Name, mchan}
	tsm, ok := <- mchan
	if ok {
		if tsm.Finished {
			if len(tsm.Error) == 0 {
				return true, errors.New(tsm.Error)
			}
			return true, nil
		} else {
			if len(tsm.Error) == 0 {
				return false, errors.New(tsm.Error)
			}
			return false, nil
		}
	}
	return false, nil
}

func (store *LocalStorage) getInducedTransformDependents(itransform types.InducedTransform) (ids []string, err error) {
	ids = make([]string,0)
	sources := make([]string,0)
	if itransform.InputStatesIDs != nil {
		for _, dgs := range itransform.InputsIDs {
			if dgs != nil {
				for _, dg := range dgs {
					data, err := elastic.GetDataGroup(string(dg.Id))
					if err != nil {
						return ids, err
					}
					sources = append(sources, data.Source)
				}
			}
		}
	}
	
	for _, source := range sources {
		if strings.Contains(source, ".") {
			continue
		}
		ids = append(ids, source)
	}
	return
}

func (store *LocalStorage) Run(itransformId string) (err error) {
	done, err := store.IsDone(itransformId)
	if err != nil {
		return
	}
	if done {
		return
	}
	
	itransform, err := elastic.GetInducedTransform(itransformId)
	if err != nil {
		return
	}
	sourceIds, err := store.getInducedTransformDependents(itransform)
	if err != nil {
		return
	}
	//for _, sourceId := range sourceIds {
	//	err := 
	//}
	//finish

	protoml_folder, err := utils.ProtoMLDir()
	if err != nil {
		return err
	}

	runDir := store.getKeyPath(InducedTransformKey(itransformId))
	err = osutils.TouchDir(runDir)
	if err != nil {
		return err
	}
	
	// Put the JSON of the induced transform and log into the given run folder
	params, err := json.Marshal(itransform)
	if err != nil {
		return err
	}
	params_path := path.Join(runDir, TASK_PARARMS_FILE)
	params_file, err := osutils.TouchFile(params_path)
	if err != nil {
		return err
	}
	_, err = params_file.Write(params)
	if err != nil {
		return err
	}
	params_file.Close()
	log_path := path.Join(runDir, TASK_LOG_FILE)
	log_file, err := osutils.TouchFile(log_path)
	if err != nil {
		return err
	}
	//defer log_file.Close()

	// Execute the Luigi Task
	// Get the path of the Luigi task
	luigi_path := path.Join(protoml_folder, LUIGI_TASK)
	task := exec.Command(luigi_path, "--directory", runDir, "--run_context", itransform.Exec, "--params_file", params_path)
	task.Stdout = log_file
	task.Stderr = log_file
	task.Start()
	
	store.LuigiTaskInsert <- TaskInsert{itransformId, itransform.Name, task}
	return
}

// execute entire pipeline
func (store *LocalStorage) Execute() (err error) {

	return
}

// get log file for transform
func (store *LocalStorage) GetTransformLogFile(transformId string) (paths string, err error) {

	return
}

// get graph id vertices and id edges
func (store *LocalStorage) GetGraph() (graph types.ProtoMLGraph, err error) {
	graph.Vertices = make([]types.ProtoMLVertex,0)
	graph.Edges = make([]types.ProtoMLEdge,0)
	dataIds, err := elastic.ElasticGetAll(elastic.DATAGROUP_TYPE)
	if err != nil {
		return
	}
	dataSet := make(map[types.ElasticID]bool)
	for _, id := range dataIds {
		dataSet[types.ElasticID(id)] = true
	}
	itransformIds, err := elastic.ElasticGetAll(elastic.INDUCED_TRANSFORM_TYPE)
	if err != nil {
		return
	}
	itransformSet := make(map[types.ElasticID]bool)
	for _, id := range itransformIds {
		itransformSet[types.ElasticID(id)] = true
	}
	stateIds, err := elastic.ElasticGetAll(elastic.STATE_TYPE)
	if err != nil {
		return
	}
	stateSet := make(map[types.ElasticID]bool)
	for _, id := range stateIds {
		stateSet[types.ElasticID(id)] = true
	}

	// add data, transform, state
	for dataId, _ := range dataSet {
		graph.Vertices = append(graph.Vertices, types.NewProtoMLVertex(elastic.DATAGROUP_TYPE, dataId))
	}	
	for itransformId, _ := range itransformSet {
	 	graph.Vertices = append(graph.Vertices, types.NewProtoMLVertex(elastic.INDUCED_TRANSFORM_TYPE, itransformId))
	}

	for stateId, _ := range stateSet {
		graph.Vertices = append(graph.Vertices, types.NewProtoMLVertex(elastic.STATE_TYPE, stateId))
	}

	for id, _ := range itransformSet {
		itransform, err := elastic.GetInducedTransform(string(id))
		if err != nil {
			return graph, err
		}
		if itransform.InputsIDs != nil {
			// add input -> transform edges
			for _, dgs := range itransform.InputsIDs {
				if dgs != nil {
					for _, dg := range dgs {
						if _, ok := dataSet[dg.Id]; !ok {
							err = errors.New(fmt.Sprintf("Transform %s takes in datagroup that does not exist, its id is %s", dg.Id))
							return graph, err
						} else {
							edge := types.NewProtoMLEdge(elastic.DATAGROUP_TYPE, dg.Id, elastic.INDUCED_TRANSFORM_TYPE, id)
							graph.Edges = append(graph.Edges, edge)
						}						
					}
				}
			}
		}
		if itransform.OutputsIDs != nil {
			// add transform -> output
			for _, dgs := range itransform.OutputsIDs {
				if dgs != nil {
					for _, oid := range dgs {
						if _, ok := dataSet[oid]; !ok {
							err = errors.New(fmt.Sprintf("Transform %s outputs datagroup that does not exist its id is %s", oid))
							return graph, err
						} else {
							edge := types.NewProtoMLEdge(elastic.INDUCED_TRANSFORM_TYPE, id, elastic.DATAGROUP_TYPE, oid)
							graph.Edges = append(graph.Edges, edge)
						}
					}
				}
			}
		}
		
		if itransform.InputStatesIDs != nil {
			// add state -> transform input
			for _, sid := range itransform.InputStatesIDs {
				if _, ok := stateSet[sid]; !ok {
					err = errors.New(fmt.Sprintf("Transform %s takes in a state that does not exist its id is %s", sid))
					return graph, err
					
				} else {
					edge := types.NewProtoMLEdge(elastic.STATE_TYPE, sid, elastic.INDUCED_TRANSFORM_TYPE, id)
					graph.Edges = append(graph.Edges, edge)
				}
			}
		}
		if itransform.OutputStatesIDs != nil {
			// add state -> transform input
			for _, sid := range itransform.OutputStatesIDs {
				if _, ok := stateSet[sid]; !ok {
					err = errors.New(fmt.Sprintf("Transform %s takes in a state that does not exist its id is %s", sid))
					return graph, err
					
				} else {
					
					edge := types.NewProtoMLEdge(elastic.INDUCED_TRANSFORM_TYPE, id, elastic.STATE_TYPE, sid)
					graph.Edges = append(graph.Edges, edge)
				}
			}
		}
	}

	return
}



// add induced transform
func (store *LocalStorage) AddInducedTransform(itransform types.InducedTransform) (itransformID string, err error) {
	logger.LogDebug(LOGTAG, "Adding Induced Transform named (%s) from transform id (%s)", itransform.Name, itransform.TemplateID)
	// Get transform template
	// parse and validate induced transform
	err = persistparsers.ValidateInducedTransform(itransform)
	if err != nil {
		itransform.Error = fmt.Sprintf("%s",err)
	} else {
		itransform.Error = ""
	}

	// add induced transform into elastic search
	itransformID, err = elastic.AddInducedTransform(itransform)
	if err != nil {
		return
	}
	logger.LogDebug(LOGTAG, "Result Induced Transform ID: %s", itransformID)	
	return
}

// update induced transform
func (store *LocalStorage) UpdateInducedTransform(itransformId string, itransform types.InducedTransform) (err error) {
	logger.LogDebug(LOGTAG, "Updating Induced Transform named (%s) from transform id (%s)", itransform.Name, itransform.TemplateID)
	// Get transform template
	// parse and validate induced transform
	err = persistparsers.ValidateInducedTransform(itransform)
	if err != nil {
		itransform.Error = fmt.Sprintf("%s",err)
	} else {
		itransform.Error = ""
	}

	// add induced transform into elastic search
	err = elastic.UpdateInducedTransform(itransformId, itransform)
	if err != nil {
		return
	}
	logger.LogDebug(LOGTAG, "Result Updated Induced Transform ID: %s", itransformId)	
	return
}
	

// load a transform from a file
func (store *LocalStorage) AddTransformFile(transformFile string) (transform types.Transform, transformID string, err error) {
	logger.LogDebug(LOGTAG, "Adding Transform from %s", transformFile)
	jsonBlob, err := osutils.LoadBlob(transformFile)
	if err != nil {
		return
	}
	
	// parse and validate transform
	transform, err = persistparsers.ParseTransform(jsonBlob)
	if err != nil {
		return transform, "", errors.New(fmt.Sprintf("Parse Error In Transform %s: %s",transformFile, err))
	}
	transform.Template = transformFile
	logger.LogDebug(LOGTAG, "\tTransform parsed")

	// add transform into elastic search
	transformID, err = elastic.AddTransform(transform)
	if err != nil {
		return
	}
	logger.LogDebug(LOGTAG, "Result Transform ID: %s", transformID)
	return
}

// reprents the physical data columns of each datagroup
type dataGroupParts struct {
	ParentGroupId string
	ColPaths []string
}
const DATAGROUPPARTS_TYPE = "dataparts"


// insert data file into persist
func (store *LocalStorage) AddDataFile(dataFile types.DatasetFile) (dataID []string, err error) {
	logger.LogDebug(LOGTAG, "Adding dataset file %s", dataFile.Path)
	// validate all dataset datatypes exist
	for typename, _ := range dataFile.Columns.ExclusiveTypes {
		if _, err := elastic.GetDataType(typename); err != nil {
			return dataID, err
		}
	}
	
	// setup dataset dir
	keyPath := store.getKeyPath(DatasetFileKey(dataFile))
	err = osutils.TouchDir(keyPath)
	if err != nil {
		return
	}

	// split dataset into data groups, column files, and array index map from file to data group
	dataGroups, colPaths, groupToCols, err := store.FormatCollection.Split(dataFile, keyPath)
	if err != nil {
		return
	}
	 
	// add data groups into elasticsearch
	dataID = make([]string,len(dataGroups))
	for i, dataGroup := range dataGroups {
		id, err := elastic.AddDataGroup(dataGroup)
		if err != nil {
			return []string{}, err
		}
		dataID[i] = id
	}

	// setup data group dirs
	dataDirs := make([]string,len(dataGroups))
	logger.LogDebug(LOGTAG, "Result Data Ids:")
	for i, id := range dataID {
		logger.LogDebug(LOGTAG, "\t%s",id)
		dataDirs[i] = store.getKeyPath(DataKey(id))
		err = osutils.TouchDir(dataDirs[i])
		if err != nil {
			return
		}
	}

	
	// construct dataparts
	dataParts := make([]dataGroupParts, len(dataGroups))
	for i, dataGroup := range dataGroups {
		id := dataID[i]  
		colGroupPaths := make([]string,len(groupToCols[i]))
		// move group cols into group dir
		for gi, ci := range groupToCols[i] {
			colPath := colPaths[ci]
			colGroupPaths[gi] = path.Join(dataDirs[i], fmt.Sprintf("%010d.%s",gi,dataGroup.FileFormat))
			err = os.Rename(colPath, colGroupPaths[i])
			if err != nil {
				return dataID, err
			}
		}
		dataParts[i] = dataGroupParts{id,colGroupPaths}
	}

	// add data parts into elastic
	logger.LogDebug(LOGTAG,"Separated DataPart Ids:")
	for _, datapart := range dataParts {
		id, err := elastic.ElasticAdd(DATAGROUPPARTS_TYPE, datapart)
		if err != nil {
			return dataID, err
		}
		logger.LogDebug(LOGTAG, "\t%s",id)
	}

	return dataID, nil
}
