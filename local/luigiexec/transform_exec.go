package luigiexec

import (
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/utils"
	//"github.com/ProtoML/ProtoML/logger"
	"github.com/ProtoML/ProtoML/utils/osutils"
	"encoding/json"
	"os/exec"
	"path"
	"errors"
)

const LOGTAG = "EXECUTOR"
const LOGFILE = "log"

func ExecTransforms (transforms []types.InducedTransform, directories []string) (err error) {
	// Get list of (checked) induced transforms in topological order
	// Go through the list in reverse order and execute the luigi tasks, so the dependency tree is all set to go
	// This way we can just finish and exit with a map of the transforms to some way to track them in Luigi
	if len(transforms) != len(directories) {
		err = errors.New("Directories and Transforms don't match up")
		return
	}

	tasks := make([]*exec.Cmd, len(transforms))
	for i, transform := range transforms {
		// For each InducedTransform, write it as JSON, then pass it to the Luigi task.
		// utils gives us the ProtoML directory 
		protoml_folder, err := utils.ProtoMLDir()
		if err != nil {
			return err
		}
		
		// Put the JSON of the induced transform and log into the given run folder
		params, err := json.Marshal(transform)
		if err != nil {
			return err
		}
		params_path := path.Join(directories[i],"params")
		params_file, err := osutils.TouchFile(params_path)
		defer params_file.Close()
		if err != nil {
			return err
		}
		_, err = params_file.Write(params)
		if err != nil {
			return err
		}
		log_path := path.Join(directories[i],LOGFILE)
		log_file, err := osutils.TouchFile(log_path)
		defer log_file.Close()
		if err != nil {
			return err
		}

		// Execute the Luigi Task
		// Get the path of the Luigi task
		luigi_path := path.Join(protoml_folder,"ProtoML-persist/local/fiber/TransformTask.py")
		tasks[i] = exec.Command(luigi_path, "--directory", directories[i], "--run_context", transform.Exec, "--params_file", params_path)
		tasks[i].Stdout = log_file
		tasks[i].Stderr = log_file
		tasks[i].Start()
	}
	return
}


