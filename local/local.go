package local

import (
	"os"
	"github.com/ProtoML/ProtoML/parsers"
	"github.com/ProtoML/ProtoML/types"
	"io/ioutil"
	"strings"
	"errors"
//	"github.com/ProtoML/ProtoML/utils"
	"fmt"
	"time"
	"path"
)

const (
	STATE_DIR = ".protoml"
	GRAPH_FILE = "proto_graph.json"
	STORAGE_DIR = ".storage"
	TRANSFORM_PREFIX = "transform-"
)

type LocalStorage struct {
	Config parsers.Config
	StateDir string
	GraphFile string
	StorageDir string
	TransformDirs map[string]string
}

func TouchDir(dir string) (err error) {
	err = os.Mkdir(dir, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		return
	} else {
		err = nil
		return
	}
}

func TouchFile(filepath string) (file* os.File, err error) {
	file, err = os.Create(filepath)
	if err != nil && !os.IsExist(err) {
		return
	} else {
		err = nil
		return
	}
}

func (base *LocalStorage) Init(config parsers.Config) (pipeline *types.Pipeline, err error) {
	base.Config = config

	_, err = os.Lstat(base.Config.RootDir)
	if err != nil {
		return
	}
	for _, templateDir := range(base.Config.Parameters.TemplatePaths) {
		_, err = os.Lstat(templateDir)
		if err != nil {
			return pipeline, err
		}
	}
	
	base.StateDir = path.Join(base.Config.RootDir,STATE_DIR)
	base.StorageDir = path.Join(base.Config.RootDir,STORAGE_DIR)
	err = TouchDir(base.StateDir)
	if err != nil {
		return
	}
	err = TouchDir(base.StorageDir)
	if err != nil {
		return
	}
	base.GraphFile = path.Join(base.StateDir,GRAPH_FILE)
	file, err := TouchFile(base.GraphFile)
	if err != nil {
		return
	}
	defer file.Close()

	pipelineBlob, err := ioutil.ReadAll(file)
	if err != nil {
		return
	}
	
	if len(pipelineBlob) > 0 {
		pipeline, err = parsers.LoadPipeline(pipelineBlob)
		if err != nil {
			return
		}
	} else { 
		pipeline = parsers.NewPipeline()
	}

	storageDir, err := os.Open(base.StorageDir)
	if err != nil {
		return
	}
	defer storageDir.Close()
	transforms, err := storageDir.Readdirnames(0)
	if err != nil {
		return
	}
	
	base.TransformDirs = make(map[string]string)
	for _, transformDir := range(transforms) {
		transformId := strings.TrimPrefix(transformDir,TRANSFORM_PREFIX)
		base.TransformDirs[transformId] = path.Join(base.StorageDir,transformDir)
	}
	return
}

func (base *LocalStorage) SaveState(pipelineBlob []byte) (err error) {
	file, err := os.OpenFile(base.GraphFile, os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	fmt.Println(file)
	if err != nil {
		return
	}
	defer file.Close()

	_, err = file.Write(pipelineBlob)
	return
}

func (base *LocalStorage) Close() (err error) {
	return
}

func (base *LocalStorage) LoadTransformDir(id string) (transformDir string, err error) {
	transformDir, ok := base.TransformDirs[id]
	if ok {
		_, err = os.Lstat(transformDir)
	} else {
		err = errors.New(fmt.Sprintf("id %v missing transform directory", id))
	}
	return
}

func (base *LocalStorage) Load(id string, filename string) (file *os.File, err error) {
	transformDir, err := base.LoadTransformDir(id)
	if err != nil {
		return
	}
	file, err = os.Open(path.Join(transformDir,filename))
	return
}

func (base *LocalStorage) Store(id string, filename string, data []byte) (err error) {
	transformDir, err := base.LoadTransformDir(id)
	if err != nil {
		return
	}
	file, err := os.OpenFile(path.Join(transformDir,filename), os.O_TRUNC, 0666)
	if err != nil {
		return
	}
	defer file.Close()

	_, err = file.Write(data)
	return	
}

func (base *LocalStorage) GenerateTransformId() (id string, err error) {
//	base := *base
	id = string(time.Now().Unix())
	return
}

func (base *LocalStorage) CreateTransform() (id string, err error) {
	transformDir := base.StorageDir+"\\"+TRANSFORM_PREFIX+id
	err = os.Mkdir(transformDir, os.ModeDir)
	return
}

func (base *LocalStorage) DeleteTransform(id string) (err error) {
	transformDir, err := base.LoadTransformDir(id)
	if err != nil {
		return
	}
	err = os.Remove(transformDir)
	return
}


