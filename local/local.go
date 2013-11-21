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
)

const (
	LOGTAG                        = "Persist-Local"
	BASE_STATE_DIRECTORY          = ".ProtoML"
	ELASTIC_DIRECTORY             = "elasticsearch"
	PROTOML_TRANSFORMS_DIRECTORY  = "ProtoML-transforms/transforms"
	DIRECTORY_DEPTH               = 4
	HEX_CHARS_PER_DIRECTORY_LEVEL = 4
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

func TransformInputKey(transformid string) string {
	return transformid + ".transform.input"
}

func TransformRunKey(transformid string) string {
	return transformid + ".transform.run"
}

func TransformOutputKey(transformid string) string {
	return transformid + ".transform.output"
}

func StateKey(stateid string) string {
	return stateid + ".state"
}

type DataFile struct {
	DataId string
	Path string
}

type LocalStorage struct {
	Config           persist.Config
	ElasticProcess   *exec.Cmd
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
	return 
}

func (store *LocalStorage) Close() (err error) {
	logger.LogInfo(LOGTAG,"Closing persistance")
	if store.ElasticProcess != nil {
		err = store.ElasticProcess.Process.Kill()
		err = store.ElasticProcess.Wait()
	}
	return
}

func (store *LocalStorage) IsDone(transformId string) bool {
	/*	directory := store.fullDirectoryPath(transformId)
			files, err := listFiles(directory)
			if err != nil {
				return false
			}
			// search for state name
		//	transformModel := persist.ModelName(transformId)
			for _, file := range files {
		//		if file == transformModel {
		//			return true
		//		}
			}*/
	return false
}

func (store *LocalStorage) Run(transformId string) (err error) {
	fullDirectoryPath := store.getKeyPath(transformId)
	err = os.MkdirAll(fullDirectoryPath, 0666)
	if err != nil {
		return err
	}
	/*
		TODO fill in:
			1. check if train or test
			if train:
				2. fill in model like normal
				3. fill in args
				4. run
				5. create new data jsons/db entries with appropriate definitions
			if test:
				2. fill in model from train namespace
				3. fill in args
				4. set test flag
				5. run
				6. create new data jsons/db entries with appropriate definitions
	*/
	command := exec.Command("name", "arg", "arg...")
	err = command.Run()
	// TODO change written files to read only
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

// add induced transform
func AddInducedTransform(itransform types.InducedTransform) (itransformID string, err error) {
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
