package persist

import (
	"io"
	"github.com/ProtoML/ProtoML/parsers"
)

type PersistCreator interface {
	CreateDirectory(dir string) (err error)
	CreateFile(dir, filename string) (err error)
}

type PersistDeleter interface {
	DeleteDirectory(dir string) (err error)
	DeleteFile(dir, filename string) (err error)
}

type PersistLoader interface {
	Load(dir, filename string) (data io.Reader, err error)
}

type PersistStorer interface {
	Store(dir, filename string, data io.Reader) (err error)
}

type PersistLister interface {
	ListDirectories() (list []string, err error)
	ListFiles(dir string) (list []string, err error)
}

type PersistStorage interface {
	Init(config parsers.Config) (err error)
	Close() (err error)
	PersistCreator
	PersistDeleter
	PersistLoader
	PersistStorer
	PersistLister
}

