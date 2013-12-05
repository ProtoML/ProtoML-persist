package persist

import (
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/formatadaptor"
	"github.com/ProtoML/ProtoML-persist/persist/elastic"
	"strings"
	"github.com/ProtoML/ProtoML/utils/osutils"
	"path"
)

type LocalPersistStorageConfig struct {
	RootDir string
	StateDir string
	ElasticPort  int
	DatasetDirectory string
	InputFiles []types.DatasetFile
}

type Config struct {
	TrainNamespace string
	ExternalTransformDirectories string
	LocalPersistStorage LocalPersistStorageConfig
	FormatCollection *formatadaptor.FileFormatCollection
}
  
type PersistStorage interface {
	// Initialize file structure / databases
	Init(config Config) error
	// Close all resources for storage
	Close() error

	// check if transform has been computed
	//IsDone(transformId string) (bool, error)
	// runs the induced transform
	Run(itransformId string) error
	// execute entire pipeline
	Execute() error
	// get log file for transform
	//GetTransformLogFile(transformId string) (string, error)

	// get graph id vertices and id edges
	GetGraph() (types.ProtoMLGraph, error)

	// add induced transform
	AddInducedTransform(itransform types.InducedTransform) (itransformID string, err error)
	// update induced transform
	UpdateInducedTransform(itransformId string, itransform types.InducedTransform) (err error)
	// delete induced transform
	//DeleteInducedTransform(itransformId string) (err error)

	// insert data on a tranform from a file
	AddTransformFile(transformFile string) (transform types.Transform, transformID string, err error)
	// insert data file into persist
	AddDataFile(dataFile types.DatasetFile) (dataID []string, err error)
}

func AddDataTypes(datatypes []types.DataType) (err error) {
	for _, datatype := range datatypes {
		_, err := elastic.AddDataType(datatype)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetTransformFiles(transformDir string) (transformFiles []string, err error) {
	dirFiles, err := osutils.ListFilesInDirectory(transformDir)
	if err != nil {
		return
	}
	transformFiles = make([]string, 0)
	for _, file := range dirFiles {
		if strings.HasSuffix(file,".json") {
			transformFiles = append(transformFiles,path.Join(transformDir,file))
		}
	}
	return 
}
