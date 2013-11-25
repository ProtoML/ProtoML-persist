package persistparsers

import (
	"fmt"
	"errors"
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/utils/osutils"
	"github.com/ProtoML/ProtoML-persist/persist"
	"github.com/ProtoML/ProtoML-persist/persist/elastic"
	"encoding/json"
	"github.com/ProtoML/ProtoML/types/constraintchecker"
)

const (
	//LOGTAG = "PersistParser"
)

func ValidateParameterConstraints(ind map[string]types.InducedParameter, primary, function map[string]types.TransformParameter, template string) (err error) {
	for param, val := range(ind) {
		if _, ok := function[param]; !ok {
			if _, ok := primary[param]; !ok {
				// Couldn't find the specified parameter in the primary or the function
				err = errors.New(fmt.Sprintf("Induced Parameter %s not found in template %s", template))
				break
			} else {
				err = constraintchecker.CheckParam(ind, primary, function, primary[param], val)
			}
			err = constraintchecker.CheckParam(ind, primary, function, function[param], val)
		}
	}
	return err
}

func ValidateHyperParameterConstraints(ind map[string]types.InducedHyperParameter, primary, function map[string]types.TransformHyperParameter, template string) (err error) {
	for param, val := range(ind) {
		if _, ok := function[param]; !ok {
			if _, ok := primary[param]; !ok {
				// Couldn't find the specified parameter in the primary or the function
				err = errors.New(fmt.Sprintf("Induced Parameter %s not found in template %s", template))
				break
			} else {
				err = constraintchecker.CheckHyper(ind, primary, function, primary[param], val)
			}
			// TODO: Handle optional parameters
			err = constraintchecker.CheckHyper(ind, primary, function, function[param], val)
		}
	}
	return err
}

func ValidateFileConstraints(ind map[string]types.InducedFileParameter, primary, function map[string]types.FileParameter, template string) (err error) {
	for param, val := range(ind) {
		if _, ok := function[param]; !ok {
			if _, ok := primary[param]; !ok {
				// Couldn't find the specified parameter in the primary or the function
				err = errors.New(fmt.Sprintf("Induced Parameter %s not found in template %s", template))
				break
			} else {
				err = constraintchecker.CheckFile(ind, primary, function, primary[param], val)
			}
			err = constraintchecker.CheckFile(ind, primary, function, function[param], val)
		}
	}
	return err
}

func ValidateStateConstraints(ind map[string]types.InducedStateParameter, primary, function map[string]types.StateParameter, template string) (err error) {
	for param, val := range(ind) {
		if _, ok := function[param]; !ok {
			if _, ok := primary[param]; !ok {
				// Couldn't find the specified parameter in the primary or the function
				err = errors.New(fmt.Sprintf("Induced Parameter %s not found in template %s", template))
				break
			} else {
				err = constraintchecker.CheckState(ind, primary, function, primary[param], val)
			}
			err = constraintchecker.CheckState(ind, primary, function, function[param], val)
		}
	}
	return err
}

func ValidateTransformFunctions(tf map[string]types.TransformFunction) (err error) {
	for name, function := range(tf) {
		if name == "" {
			err = errors.New(fmt.Sprintf("Empty function name for function &#v",function))
		} else if function.Description == "" {
			err = errors.New(fmt.Sprintf("No Description for function %s", name))
		}
		if err != nil {
			return
		}
	}
	return
}

func ValidateTransform(temp types.Transform) (err error) {
	if temp.Name == "" {
		err = errors.New("No transform name")
	} else if len(temp.Documentation) > 0 {
		err = errors.New("Template field is only to be filled by server")
	} else if temp.Documentation == "" {
		err = errors.New("No Documentation")
	} else if temp.Functions == nil {
		err = errors.New("No Functions")
	} else if len(temp.Functions) < 1 {
		err = errors.New("Must have at least one function in template")
	}
	err = ValidateTransformFunctions(temp.Functions)
	return err
}


func ParseTransform(templateJSON []byte) (transform types.Transform, err error) {
	err = json.Unmarshal(templateJSON, &transform)
	if err != nil { return }
	err = ValidateTransform(transform)
	return
}

func ParseInducedTransform(templateJSON []byte) (itransform types.InducedTransform, err error) {
	err = json.Unmarshal(templateJSON, &itransform)
	if err != nil { return }
	err = ValidateInducedTransform(itransform)
	return
}

func ValidateInducedTransform(indt types.InducedTransform) (err error) {
	// First, get the template transform from elastic
	against, err := elastic.GetTransform(string(indt.TemplateID))
	if err != nil {
		err = errors.New(fmt.Sprintf("Invalid TemplateID %s", indt.TemplateID))
		return
	}
	if len(indt.Name) == 0 {
		err = errors.New("No name in induced transform")
	} else if with, ok := against.Functions[indt.Function]; !ok {
		err = errors.New(fmt.Sprintf("Function not in template %s", against.Template))
	} else if err = ValidateParameterConstraints(indt.Parameters, against.PrimaryParameters, with.Parameters, against.Template); err != nil {
	} else if err = ValidateHyperParameterConstraints(indt.HyperParameters, against.PrimaryHyperParameters, with.HyperParameters, against.Template); err != nil {
	} else if err = ValidateFileConstraints(indt.Inputs, against.PrimaryInputs, with.Inputs, against.Template); err != nil {
	} else if err = ValidateFileConstraints(indt.Outputs, against.PrimaryOutputs, with.Outputs, against.Template); err != nil {
	} else if err = ValidateStateConstraints(indt.InputStates, against.PrimaryInputStates, with.InputStates, against.Template); err != nil {
	} else if err = ValidateStateConstraints(indt.OutputStates, against.PrimaryOutputStates, with.OutputStates, against.Template); err != nil {
	}
	return
}
func LoadConfig(configFile string) (config persist.Config, err error) {
	jsonBlob, err := osutils.LoadBlob(configFile)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonBlob, &config)
	if err != nil {
		return
	}
	return
}

func ValidateDatasetFile(dataFile types.DatasetFile) (err error) {
	// validation
	if len(dataFile.Path) == 0 {
		err = errors.New("No path in datafile specification")
	} else if len(dataFile.FileFormat) == 0 {
		err = errors.New("No file format in datafile specification")
	} else if dataFile.NRows == 0 {
		err = errors.New("No rows size in datafile specification")
	} else if dataFile.NRows < 0 {
		err = errors.New("Negative rows size in datafile specification")
	} else if dataFile.NCols == 0 {
		err = errors.New("No columns size in datafile specification")
	} else if dataFile.NCols < 0 {
		err = errors.New("Negative columns size in datafile specification")
	} else if len(dataFile.Columns.ExclusiveTypes) == 0 {
		err = errors.New("No exclusive in datafile specification")
	} else if len(dataFile.Columns.Tags) == 0 {
		err = errors.New("No tags in datafile specification")
	}
	if err != nil {
		return err
	}

	// validate exclusive types fit correct indices
	sumColIndexes := 0
	for etype, indices := range dataFile.Columns.ExclusiveTypes {
		for _, index := range indices{
			if index < 0 {
				err = errors.New(fmt.Sprintf("Type %s has an index below 0", etype))
				return
			} 
			if index >= dataFile.NCols {
				err = errors.New(fmt.Sprintf("Type %s has an index not in range [0,number of cols)", etype))
				return
			}
			sumColIndexes += index
		}
	}
	if sumColIndexes > (dataFile.NCols-1)*dataFile.NCols/2 {
		err = errors.New("Too many indices compared to datafile column size specification")
	} else if sumColIndexes < (dataFile.NCols-1)*dataFile.NCols/2 {
		err = errors.New("Not enough indices compared to datafile column size specification")
	}
	if err != nil {
		return
	}

	// validate tags fit in correct indices
	for tag, indices := range dataFile.Columns.Tags {
		for _, index := range indices {
			if index < 0 {
				err = errors.New(fmt.Sprintf("Tag %s has an index below 0", tag))
				return
			} 
			if index >= dataFile.NCols {
				err = errors.New(fmt.Sprintf("Tag %s has an index not in range [0,number of cols)", tag))
				return
			}
		}
	}

	return nil
}
 
func ParseDatasetFile(jsonBlob []byte) (dataFile types.DatasetFile, err error) {
	err = json.Unmarshal(jsonBlob, &dataFile)
	if err != nil {
		return
	}
	err = ValidateDatasetFile(dataFile)
	return
}



/*func ParseExternalTransforms(store PersistStorage, externalTransformDirectories []string) (transforms []types.Transform, err error) {
=======
// Doesn't build, no persist storage
func ParseExternalTransforms(store PersistStorage, externalTransformDirectories []string) (transforms []types.Transform, err error) {
>>>>>>> zmjjmz/master
	transforms = make([]types.Transform, 0)
	for _, transformDir := range externalTransformDirectories {
		dir, err := os.Open(transformDir)
		if err != nil {
			return transforms, err
		}
		defer dir.Close()
		files, err := dir.Readdir(0)
		for _, file := range files {
			// load all json files in directory as transforms
			if !file.IsDir() && strings.HasSuffix(strings.ToLower(file.Name()), ".json") {
				jsonBlob, err := osutils.LoadBlob(path.Join(transformDir, file.Name()))
				if err != nil {
					return transforms, err
				}
				transform, err := ParseTransform(jsonBlob)
				if err != nil {
					return transforms, err
				}
				transforms = append(transforms, transform)
			}
		}
	}
	return
}
*/
