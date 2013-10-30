package elastic

import (
	"github.com/ProtoML/ProtoML/types"
	"github.com/mattbaird/elastigo/core"
	"errors"
	"fmt"
	"encoding/json"
	"github.com/ProtoML/ProtoML/logger"
	"time"
)

const (
	LOGTAG                        = "ElasticSearch"
	PROTOML_INDEX                 = "protoml"
	DATATYPE_TYPE                 = "datatype"
	DATAGROUP_TYPE                = "data"
	DATAFILE_TYPE                 = "datafile"
	TRANSFORM_TYPE                = "transform"
	STATE_TYPE                    = "state"
)

func ElasticGetError(res core.SearchResult,  errormsg string) (err error) {
	if res.TimedOut {
		err = errors.New(fmt.Sprintf("elasticsearch Get for %s timed out",errormsg))
		return
	}
	if res.Hits.Total == 0 {
		err = errors.New(fmt.Sprintf("elasticsearch Get for %s returned no results",errormsg))
		return
	}
	return nil
}


func ElasticAdd(elastictype string, data interface{}) (id string, err error) {
	// index
	resp, err := core.Index(true, PROTOML_INDEX, elastictype, "", data)
	if err != nil {
		return
	}
	if !resp.Ok {
		err = errors.New(fmt.Sprintf("elastic Addtion of type %s failed", elastictype))
	}
	time.Sleep(time.Second) // sleep to allow for elasticsearch indexing
	return
}

func GetDataType(name types.DataTypeName) (datatype types.DataType, err error) {
	// search 
	res, err := core.SearchUri(PROTOML_INDEX, DATATYPE_TYPE, fmt.Sprintf("TypeName=%s",name), "", 0)
	if err != nil {
		return
	}
	err = ElasticGetError(res, fmt.Sprintf("datatype %s",name))
	if err != nil {
		return
	}

	// unmarshall search
	hit := res.Hits.Hits[0]
	err = json.Unmarshal(hit.Source,&datatype)
	return
}

func AddDataType(datatype types.DataType) (id string, err error) {
	logger.LogDebug(LOGTAG,"Adding DataType named %s", datatype.TypeName)
	// validate parents exist
	for _, parent := range datatype.ParentTypes {
		if _, err := GetDataType(parent); err != nil {
			return id, err
		}
	}
	return ElasticAdd(DATATYPE_TYPE, datatype)
}
