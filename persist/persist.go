package persist

/*
   Parser for configuration files.
*/

import (
	"encoding/json"
	"fmt"
	"github.com/ProtoML/ProtoML-core/utils"
	"os"
	"path"
)

var PROTOMLPATH string

func init() {
	PROTOMLPATH = os.Getenv("PROTOMLPATH")
	if PROTOMLPATH == "" {
		PROTOMLPATH = "."
	}
}

func ConfigValue(key string) interface{} {
	config := make(map[string]interface{})
	utils.HandleError(JsonDecoder(PROTOMLPATH, "config").Decode(&config))
	return config[key]
}

func StringConfig(key string) string {
	val, ok := ConfigValue(key).(string)
	utils.Assert(ok, fmt.Sprintf("Cannot convert %s to string.\n", key))
	return val
}

func JsonDecoder(folder string, filename string) *json.Decoder {
	full_path := path.Join(folder, filename+".json")
	file, err := os.OpenFile(full_path, os.O_RDONLY, 0644)
	utils.HandleError(err)
	return json.NewDecoder(file)
}

// type Persister interface { // TODO fix?
// 	DataPath(id string, index uint64) string // path for output data
// 	LoadTransform(id string) Transformer
// }
