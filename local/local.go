package local

import (
	"os"
	"github.com/ProtoML/ProtoML/parsers"
//	"github.com/ProtoML/ProtoML/utils"
	"path"
	"io"
	"bufio"
)

const (
	STATE_DIR = ".protoml"
)

type LocalStorage struct {
	Config parsers.Config
	StateDir string
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

func (base *LocalStorage) Init(config parsers.Config) (err error) {
	base.Config = config
	
	// check root and template directories exist
	_, err = os.Lstat(base.Config.RootDir)
	if err != nil {
		return
	}
	for _, templateDir := range(base.Config.Parameters.TemplatePaths) {
		_, err = os.Lstat(templateDir)
		if err != nil {
			return 
		}
	}
	
	// touch state directory
	base.StateDir = path.Join(base.Config.RootDir,STATE_DIR)
	err = TouchDir(base.StateDir)

	return
}

func (base *LocalStorage) Close() (err error) {
	return nil
}

func (base *LocalStorage) CreateDirectory(dir string) (err error) {
	err = os.Mkdir(path.Join(base.StateDir,dir), os.ModePerm)
	return
}

func (base *LocalStorage) CreateFile(dir, filename string) (err error) {
	_, err = os.Create(path.Join(base.StateDir,dir,filename))
	return
}

func (base *LocalStorage) DeleteFile(dir, filename string) (err error) {
	err = os.Remove(path.Join(base.StateDir,dir,filename))
	return
}

func (base *LocalStorage) DeleteDirectory(dir string) (err error) {
	err = os.Remove(path.Join(base.StateDir,dir))
	return
}

func (base *LocalStorage) Load(dir, filename string) (data io.Reader, err error) {
	data, err = os.Open(path.Join(base.StateDir,dir,filename))
	return
}

func (base *LocalStorage) Store(dir, filename string, data io.Reader) (err error) {
	file, err := os.Create(path.Join(base.StateDir,dir,filename))
	if err != nil {
		return
	}
	defer file.Close()

	// buffered pipe into file
	bufData := bufio.NewReader(data)
	bufFile := bufio.NewWriter(file)
	
	_, err = bufFile.ReadFrom(bufData)
	if err != nil {
		return
	}
	err = bufFile.Flush()
	return	
}

func (base *LocalStorage) ListDirectories() (list []string, err error) {
	stateDir, err := os.Open(base.StateDir)
	if err != nil {
		return
	}
	defer stateDir.Close()
	list, err = stateDir.Readdirnames(0)
	return
}

func (base *LocalStorage) ListFiles(dir string) (list []string, err error) {
	dire, err := os.Open(path.Join(base.StateDir, dir))
	if err != nil {
		return
	}
	defer dire.Close()
	
	files, err := dire.Readdir(0)
	list = make([]string,len(files))
	listIter := 0
	for _, file := range(files) {
		if !file.IsDir() {
			list[listIter] = file.Name()
			listIter++
		}
	}
	return	
}


