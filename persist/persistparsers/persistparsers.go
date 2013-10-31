package persistparsers

import (
	"fmt"
	"errors"
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/utils/osutils"
	"path"
	"strings"
	"encoding/json"
	"os"
	"io/ioutil"
	"github.com/ProtoML/ProtoML/types/constraintchecker"
)




const (
	//LOGTAG = "PersistParser"
)
func ValidateTransformFunctions(template string, tf map[string]types.TransformFunction) (err error) {
	for name, function := range(tf) {
		if name == "" {
			err = errors.New(fmt.Sprintf("Empty function name in template %s",template))
		} else if function.Description == "" {
			err = errors.New(fmt.Sprintf("No Description specified for function %s in template %s", name, template))
		} else if function.Exec != "" {
			_, err = os.Stat(function.Exec)
			if err != nil {
			err = errors.New(fmt.Sprintf("Could not stat execution context %s for function %s in template %s", function.Exec, name, template))
			}
		}
	}
	return err
}

func ValidateTransform(temp types.Transform) (err error) {

	template := temp.Template
	if template == "" {
		err := errors.New(fmt.Sprintf("No template name: %#v", temp))
	}	else if temp.PrimaryParameters == nil {
		err := errors.New(fmt.Sprintf("No Primary Parameters specified in template %s", template))
	} else if temp.PrimaryHyperParameters == nil {
		err := errors.New(fmt.Sprintf("No Primary HyperParameters specified in template %s", template))
	} else if temp.PrimaryExec == "" {
		err := errors.New(fmt.Sprintf("No execution context specified in template %s", template))
	} else if _, err := os.Stat(temp.PrimaryExec); err != nil {
		err := errors.New(fmt.Sprintf("Could not stat execution context %s in template %s", temp.PrimaryExec, template))
	} else if temp.Documentation == "" {
		err := errors.New(fmt.Sprintf("No Documentation on the Transform in template %s", template))
	} else if temp.PrimaryInputs == nil {
		err := errors.New(fmt.Sprintf("No Primary Inputs specified in template %s", template))
	} else if len(temp.PrimaryInputs) < 1 {
		err := errors.New(fmt.Sprintf("Must have at least one input in template %s", template))
	} else if temp.PrimaryOutputs == nil {
		err := errors.New(fmt.Sprintf("No Primary Outputs specified in template %s", template))
	} else if len(temp.PrimaryOutputs) < 1 {
		err := errors.New(fmt.Sprintf("Must have at least one output in template %s", template))
	} else if temp.PrimaryInputStates == nil {
		err := errors.New(fmt.Sprintf("No Primary Input States specified in template %s", template))
	} else if temp.PrimaryOutputStates == nil {
		err := errors.New(fmt.Sprintf("No Primary Output States specified in template %s", template))
	} else if temp.Functions == nil {
		err := errors.New(fmt.Sprintf("No Functions specified in template %s", template))
	} else if len(temp.Functions) < 1 {
		err := errors.New(fmt.Sprintf("Must have at least one function in template %s", template))
	}
	err = ValidateTransformFunctions(template, temp.Functions)
	return err
}

func ValidateParameterConstraints(ind map[string]types.InducedParameter, primary, function map[string]types.TransformParameter, template string) (err error) {
	for param, val := range(ind) {
		if constr, ok := function[param]; !ok {
			if constr, ok := primary[param]; !ok {
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
		if constr, ok := function[param]; !ok {
			if constr, ok := primary[param]; !ok {
				// Couldn't find the specified parameter in the primary or the function
				err = errors.New(fmt.Sprintf("Induced Parameter %s not found in template %s", template))
				break
			} else {
				err = constraintchecker.CheckHyper(ind, primary, function, primary[param], val)
			}
			err = constraintchecker.CheckHyper(ind, primary, function, function[param], val)
		}
	}
	return err
}

func ValidateFileConstraints(ind map[string]types.InducedFileParameter, primary, function map[string]types.FileParameter, template string) (err error) {
	for param, val := range(ind) {
		if constr, ok := function[param]; !ok {
			if constr, ok := primary[param]; !ok {
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
		if constr, ok := function[param]; !ok {
			if constr, ok := primary[param]; !ok {
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

func ValidateInducedTransform(indt types.InducedTransform, against types.Transform) (err error) {
	if with, ok := against.Functions[indt.Function]; !ok {
		err = errors.New(fmt.Sprintf("Function not in template %s", against.Template))
	} else if err = ValidateParameterConstraints(indt.Parameters, against.PrimaryParameters, with.Parameters, against.Template); err != nil {
	} else if err = ValidateHyperParameterConstraints(indt.HyperParameters, against.PrimaryHyperParameters, with.HyperParameters, against.Template); err != nil {
	} else if err = ValidateFileConstraints(indt.Inputs, against.PrimaryInputs, with.Inputs, against.Template); err != nil {
	} else if err = ValidateFileConstraints(indt.Outputs, against.PrimaryOutputs, with.Outputs, against.Template); err != nil {
	} else if err = ValidateStateConstraints(indt.InputStates, against.PrimaryInputStates, with.InputStates, against.Template); err != nil {
	} else if err = ValidateStateConstraints(indt.OutputStates, against.PrimaryOutputStates, with.OutputStates, against.Template); err != nil {
	}
	return err
}


func ValidateDatasetFile(dataFile types.DatasetFile) (err error) {
	// validation
	if len(dataFile.Path) == 0 {
		err = errors.New("No path in datafile specification")
	} else if len(dataFile.FileFormat) == 0 {
		err = errors.New("No file format in datafile specification")
	} else if dataFile.NRows == 0 {
		err = errors.New("No rows size in datafile specification")
	} else if dataFile.NCols == 0 {
		err = errors.New("No columns size in datafile specification")
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
			if index >= int(dataFile.NCols) {
				err = errors.New(fmt.Sprintf("Type %s has an index not in range [0,number of cols)", etype))
				return
			}
			sumColIndexes += index
		}
	}
	if sumColIndexes > int((dataFile.NCols-1)*dataFile.NCols/2) {
		err = errors.New("Too many indices compared to datafile column size specification")
	} else if sumColIndexes < int((dataFile.NCols-1)*dataFile.NCols/2) {
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
			if index >= int(dataFile.NCols) {
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

func ParseTransformTemplate(templateJSON []byte) (transform types.Transform, err error) {
	err = json.Unmarshal(templateJSON, &transform)
	if err != nil { return }

	err = ValidateJSONTemplate(transform)
	return
}

func ParseInducedTransform(indJSON []byte) (indtransform types.InducedTransform, err error) {
	err = json.Unmarshal(indJSON, &indtransform)
	if err != nil {
		return
	}
	templateJSON, err := ioutil.ReadFile(indtransform.Template)
	if err != nil {
		return
	}
	temp, err := ParseTransformTemplate(templateJSON)
	if err != nil {
		return
	}
	err = ValidateInducedTransform(indtransform, temp)
	return
	
}

// Doesn't build, no persist storage
func ParseExternalTransforms(store PersistStorage, externalTransformDirectories []string) (transforms []types.Transform, err error) {
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
