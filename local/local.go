package local

import (
	"fmt"
	"github.com/ProtoML/ProtoML-persist/persist"
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/formatadaptor"
	"github.com/ProtoML/ProtoML/logger"
	//"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/utils/osutils"
	//"github.com/mattbaird/elastigo/api"
	//"github.com/mattbaird/elastigo/core"
	"os"
	"os/exec"
	"path"
	"errors"
	"time"
	"github.com/ProtoML/ProtoML-persist/persist/elastic"
)

const (
	LOGTAG                        = "Persist-Local"
	BASE_STATE_DIRECTORY          = ".ProtoML"
	ELASTIC_DIRECTORY             = "elasticsearch"
	TRANSFORM_DATA_FILE           = "TransformData.json"
	GRAPH_STRUCTURE_FILE          = "GraphStructure.json"
	AVAIABLE_TRANSFORMS_FILE      = "AvaiableTransforms.json"
	AVAIABLE_DATATYPES_FILE       = "AvaiableDataTypes.json"
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
	if store.ElasticProcess != nil {
		logger.LogInfo(LOGTAG,"Closing persistance")
		err = store.ElasticProcess.Process.Signal(os.Interrupt)
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

// returns the filename of data for the graph
func (store *LocalStorage) GraphStructureFile() (paths string, err error) {

	return
}

// add transform into graph
func (store *LocalStorage) AddGraphTransform(parentDataIDs []string, transformName string) (transformID string, err error) {

	return
}

// update transform parameters in graph
func (store *LocalStorage) UpdateGraphTransform(transformId string, parameters map[string]string) (err error) {

	return
}

// delete transform from graph
func (store *LocalStorage) RemoveGraphTransform(transformId string) (err error) {

	return
}

// insert data on a tranform from a file
func (store *LocalStorage) AddTransformFile(transformFile string) (transform types.Transform, err error) {

	return
}

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
	dataGroups, _, _, err := store.FormatCollection.Split(dataFile, keyPath)
	if err != nil {
		return
	}
	
	dataGroupId := make([]string,len(dataGroups))
	/*for i, dataGroup := range dataGroups {
		//response, err := core.Index(true,PROTOML_INDEX,DATAGROUP_TYPE, "", dataGroup)	
		if err != nil {
			return []string{}, err
		}
		//dataGroupId[i] = response.Id
	}*/
	
	logger.LogDebug(LOGTAG, "Result Data Ids:")
	for _, id := range dataGroupId {
		logger.LogDebug(LOGTAG, "%s",id)
	}
	
	return
}
