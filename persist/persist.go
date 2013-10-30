package persist

import (
	"encoding/json"
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/utils/osutils"
	"github.com/ProtoML/ProtoML/formatadaptor"
	"github.com/ProtoML/ProtoML-persist/persist/elastic"
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
	//IsDone(transformId string) bool
	// runs the transform
	//Run(transformId string) error
	// execute entire pipeline
	//Execute() error
	// get log file for transform
	//GetTransformLogFile(transformId string) (string, error)

	// returns the filename of data for the graph
	//GraphStructureFile() (string, error)
	// add transform into graph
	//AddGraphTransform(parentDataIDs []string, transformName string) (transformID string, err error)
	// update transform parameters in graph
	//UpdateGraphTransform(transformId string, parameters map[string]string) (err error)
	// reset transform to defaults
	//ResetGraphTransform(transformId string) (err error)
	// delete transform from graph
	//RemoveGraphTransform(transformId string) (err error)

	// insert data on a tranform from a file
	//AddTransformFile(transformFile string) (types.Transform, error)
	// insert data file into persist
	AddDataFile(dataFile types.DatasetFile) (dataID []string, err error)
}

func LoadConfig(configFile string) (config Config, err error) {
	jsonBlob, err := osutils.LoadBlob(configFile)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonBlob, &config)
	return
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

