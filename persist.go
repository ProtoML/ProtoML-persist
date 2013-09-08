package persist

import (
	"os"
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/parsers"
)

type PersistLoader interface {
	Load(id string, filename string) (file *os.File, err error)
}

type PersistStorer interface {
	Store(id string, filename string, data []byte) (err error)
}

type PersistManager interface {
	GenerateTransformId() (id string, err error)
	CreateTransform() (id string, err error)
	DeleteTransform(id string) (err error)
}

type Persistance interface {
	Init(config parsers.Config) (pipeline types.Pipeline, err error)
	SaveState(pipelineBlob []byte) (err error)
	Close() (err error)
	PersistLoader
	PersistStorer
	PersistManager
}
