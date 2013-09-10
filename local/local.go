package local

import (
	"github.com/ProtoML/ProtoML-persist/persist"
	"github.com/ProtoML/ProtoML/types"
	"os"
	"os/exec"
	"path"
)

const (
	BASE_STATE_DIRECTORY          = ".ProtoML"
	DB_NAME                       = "ProtoML.db"
	DIRECTORY_DEPTH               = 5
	HEX_CHARS_PER_DIRECTORY_LEVEL = 2
)

func touchDir(dir string) (err error) {
	err = os.Mkdir(dir, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		return
	} else {
		err = nil
		return
	}
}

func touchFile(filepath string) (file *os.File, err error) {
	file, err = os.Create(filepath)
	if err != nil && !os.IsExist(err) {
		return
	} else {
		err = nil
		return
	}
}

func relativeDirectoryPath(filename string) string {
	// returns the relative directory of a file
	hashed := persist.Hash(filename)
	directories := make([]string, DIRECTORY_DEPTH)
	for x := 0; x < DIRECTORY_DEPTH; x++ {
		directories[x] = hashed[HEX_CHARS_PER_DIRECTORY_LEVEL*x : HEX_CHARS_PER_DIRECTORY_LEVEL*(x+1)]
	}
	return path.Join(directories...)
}

func exists(fullPath string) bool {
	// checks if a file exists
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}

func listFiles(directory string) (list []string, err error) {
	directoryFileDescriptor, err := os.Open(directory)
	if err != nil {
		return
	}
	defer directoryFileDescriptor.Close()

	files, err := directoryFileDescriptor.Readdir(0)
	list = make([]string, len(files))
	listIter := 0
	for _, file := range files {
		if !file.IsDir() {
			list[listIter] = file.Name()
			listIter++
		}
	}
	return
}

type LocalStorage struct {
	Config persist.Config
}

func (store *LocalStorage) stateDirectory() string {
	return path.Join(store.Config.RootDir, BASE_STATE_DIRECTORY)
}

func (store *LocalStorage) fullDirectoryPath(filename string) string {
	return path.Join(store.stateDirectory(), relativeDirectoryPath(filename))
}

func (store *LocalStorage) Init(config persist.Config) (err error) {
	store.Config = config

	// check root and template directories exist
	err = touchDir(store.Config.RootDir)
	if err != nil {
		return
	}

	// touch state directory
	err = touchDir(store.stateDirectory())
	if err != nil {
		return
	}

	// will not create nested directory structure, since it can be created lazily on the fly

	// TODO add transforms to DB
	return
}

func (store *LocalStorage) IsDone(transformId string) bool {
	directory := store.fullDirectoryPath(transformId)
	files, err := listFiles(directory)
	if err != nil {
		return false
	}
	transformModel := persist.ModelName(transformId)
	for _, file := range files {
		if file == transformModel {
			return true
		}
	}
	return false
}

func (store *LocalStorage) Run(runRequest types.RunRequest) (err error) {
	// TODO store data in database: time, sum of input size, input rows, input formats, exact call, parents to update graph structure
	transformId := persist.TransformId(runRequest)
	fullDirectoryPath := store.fullDirectoryPath(transformId)
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

func (store *LocalStorage) TransformData(transformName string) (path string, err error) {
	// TODO database write to csv
	return
}

func (store *LocalStorage) GraphStructure() (path string, err error) {
	// TODO database write to csv
	return
}

func (store *LocalStorage) AvailableTransforms() (path string, err error) {
	// TODO database write to csv
	return

}
func (store *LocalStorage) LoadTransform(transformName string) (transform types.Transform, err error) {
	// TODO parse transform json into a types.Transform
	return
}

func (store *LocalStorage) LoadData(dataId string) (data types.Data, err error) {
	// TODO find data json and load into a types.Data
	return
}
