package persist

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/ProtoML/ProtoML/types"
	"io"
	"io/ioutil"
	"os"
)

type PersistStorage interface {
	// Initialize file structure
	Init(config Config) error
	// check if transform has been computed
	IsDone(transformId string) bool
	// runs the transform
	Run(types.RunRequest) error
	// returns the filename of database data for a transform
	TransformData(transformName string) (string, error)
	// returns the filename of database data for the graph
	GraphStructure() (string, error)
	// load transform from transform json
	LoadTransform(transformName string) (types.Transform, error)
	// find data json and load
	LoadData(dataId string) (types.Data, error)
}

type Config struct {
	RootDir        string
	TrainNamespace string
}

const (
	CONFIG_FILE = "ProtoML_config.json"
)

func loadBlob(filename string) (blob []byte, err error) {
	fileReader, err := os.Open(filename)
	if err != nil {
		return
	}
	blob, err = ioutil.ReadAll(fileReader)
	fileReader.Close()
	return
}

func LoadConfig() (config Config, err error) {
	jsonBlob, err := loadBlob(CONFIG_FILE)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonBlob, &config)
	return
}

func Hash(anything ...interface{}) string {
	// returns the md5 hash of anything that can be printed as a string
	h := md5.New()
	io.WriteString(h, fmt.Sprint(anything...))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TransformId(runRequest types.RunRequest) string {
	return Hash(runRequest)
}

func DataId(transformId string, index uint) string {
	return fmt.Sprintf("%s-%d", transformId, index)
}

func ModelName(transformId string) string {
	return transformId + ".model"
}
