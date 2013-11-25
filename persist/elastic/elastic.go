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
	INDUCED_TRANSFORM_TYPE        = "itransform"
	STATE_TYPE                    = "state"
)

func ElasticSearchError(res core.SearchResult, errormsg string) (err error) {
	if res.TimedOut {
		err = errors.New(fmt.Sprintf("elasticsearch Get for %s timed out",errormsg))
		return
	}
	// if res.Hits.Total == 0 {
	// 	err = errors.New(fmt.Sprintf("elasticsearch Get for %s returned no results",errormsg))
	// 	return
	// }
	return nil
}


func ElasticIndex(elastictype string, data interface{}, eid string) (id string, err error) {
	// index 
	resp, err := core.IndexWithParameters(true, PROTOML_INDEX, elastictype, eid, "", 0, "create", "", "", 0, "", "", false, data) 
	if err != nil {
		return
	}
	if !resp.Ok {
		err = errors.New(fmt.Sprintf("elastic addtion of type %s failed", elastictype))
	}
	time.Sleep(time.Second) // sleep to allow for elasticsearch indexing
	id = resp.Id
	return
}

func ElasticAdd(elastictype string, data interface{}) (id string, err error) {
	return ElasticIndex(elastictype, data, "")
}

func ElasticUpdate(elastictype string, elasticid string, data interface{}) (err error) { 
	_, err = ElasticIndex(elastictype, data, elasticid)
	return
}

func ElasticDelete(elastictype string, elasticid string) (err error) { 
	_, err = core.Delete(true, PROTOML_INDEX, elastictype, elasticid, 0, "")
	return
}

func ElasticGetAll(elastictype string) (ids []string, err error) {
	// search 
	res, err := core.SearchRequest(true, PROTOML_INDEX, elastictype, "", "", 0)
	if err != nil {
		return
	}
	err = ElasticSearchError(res, fmt.Sprintf("%s ", elastictype))
	if err != nil {
		return
	}

	hits := res.Hits.Hits
	ids = make([]string,len(hits))
	for i, hit := range hits {
		ids[i] = hit.Id
	}
	return 
}

func GetDataType(name types.DataTypeName) (datatype types.DataType, err error) {
	// search 
	res, err := core.SearchUri(PROTOML_INDEX, DATATYPE_TYPE, fmt.Sprintf("TypeName=%s",name), "", 0)
	if err != nil {
		return
	}
	err = ElasticSearchError(res, fmt.Sprintf("datatype %s",name))
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

func GetDataTypeAncestors(name types.DataTypeName) (ancestorTypes []types.DataTypeName, err error) {
	parentSearch := []types.DataTypeName{name}
	ancestorTypes = make([]types.DataTypeName, 0)
	for len(parentSearch) > 0 {
		parent := parentSearch[0]
		parentSearch = parentSearch[1:]
		// update ancestors
		dtype, err := GetDataType(parent)
		if err != nil {
			return nil, err
		}
		parentSearch = append(parentSearch, dtype.ParentTypes...)
		ancestorTypes = append(ancestorTypes, dtype.ParentTypes...)
	}
	return 
}

func IsDataTypeAncestor(childType, ancestorType types.DataTypeName) (isParent bool, err error) {
	ancestors, err := GetDataTypeAncestors(childType)
	if err != nil {
		return false, err
	}
	
	for _, ancestor := range ancestors {
		if ancestor == ancestorType {
			return true, nil
		}
	}
	return false, nil
}

func AddDataGroup(datagroup types.DataGroup) (id string, err error) {
	logger.LogDebug(LOGTAG,"Adding DataGroup of type %s with shape %d cols and %d rows", datagroup.Columns.ExclusiveType, datagroup.NCols, datagroup.NRows)
	// validate column type exists
	if _, err := GetDataType(datagroup.Columns.ExclusiveType); err != nil {
		return id, err
	}
	return ElasticAdd(DATAGROUP_TYPE, datagroup)
}

func UpdateDataGroup(eid string, datagroup types.DataGroup) (err error) {
	logger.LogDebug(LOGTAG,"Updating DataGroup of type %s with shape %d cols and %d rows", datagroup.Columns.ExclusiveType, datagroup.NCols, datagroup.NRows)
	// validate column type exists
	if _, err := GetDataType(datagroup.Columns.ExclusiveType); err != nil {
		return err
	}
	return ElasticUpdate(DATAGROUP_TYPE, eid, datagroup)
}

func AddTransform(transform types.Transform) (id string, err error) {
	logger.LogDebug(LOGTAG,"Adding Transform %s from file %s", transform.Name, transform.Template)
	// TODO validate input/output types exist
	return ElasticAdd(TRANSFORM_TYPE, transform)
}

func GetTransform(id string) (transform types.Transform, err error) {
	// search 
	res, err := core.Get(true, PROTOML_INDEX, TRANSFORM_TYPE, id)
	if err != nil {
		return
	}
	if !res.Ok {
		err = errors.New(fmt.Sprintf("elastic get failed on induced transform id %s",id))
		return
	}
	if !res.Found {
		err = errors.New(fmt.Sprintf("Can't find induced transform id %s",id))
		return
	}

	transform = res.Source.(types.Transform)
	return
}

func AddState(state types.State) (id string, err error) {
	logger.LogDebug(LOGTAG,"Adding State from source %s", state.Source)
	return ElasticAdd(STATE_TYPE, state)
}

func AddInducedTransform(itransform types.InducedTransform) (id string, err error) {
	logger.LogDebug(LOGTAG,"Adding Induced Transform %s from file %s", itransform.Name, itransform.Template)
	return ElasticAdd(INDUCED_TRANSFORM_TYPE, itransform)
}

func UpdateInducedTransform(itransformId string, itransform types.InducedTransform) (err error) {
	logger.LogDebug(LOGTAG,"Updating Induced Transform %s from file %s", itransform.Name, itransform.Template)
	return ElasticUpdate(INDUCED_TRANSFORM_TYPE, itransformId, itransform)
}

func GetInducedTransform(id string) (itransform types.InducedTransform, err error) {
	// search 
	res, err := core.Get(true, PROTOML_INDEX, TRANSFORM_TYPE, id)
	if err != nil {
		return
	}
	if !res.Ok {
		err = errors.New(fmt.Sprintf("elastic get failed on induced transform id %s",id))
		return
	}
	if !res.Found {
		err = errors.New(fmt.Sprintf("Can't find induced transform id %s",id))
		return
	}

	itransform = res.Source.(types.InducedTransform)
	return
}
