package persistparsers

import (
	"fmt"
	"errors"
	"github.com/ProtoML/ProtoML/types"
	"json"
)




const (
	//LOGTAG = "PersistParser"
)


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
		return
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

func ParseTransform(jsonBlob []byte) (transform types.Transform, err error) {
	err = json.Unmarshal(jsonBlob, &transform)
	return
}

func ParseInternalTransforms() (transforms []types.Transform, err error) {
	transforms = make([]types.Transform, 0)
	return
}

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
