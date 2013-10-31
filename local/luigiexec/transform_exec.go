package luigiexec

import (
	"github.com/ProtoML/ProtoML/types"
	"github.com/ProtoML/ProtoML/utils"
	"github.com/ProtoML/ProtoML/logger"
	"github.com/ProtoML/ProtoML/utils/osutils"
	"encoding/json"
	"os/exec"
	"path"
	"errors"
)

const LOGTAG = "Executor"
const LOGFILE = "task.log"

func ExecTransforms (transforms []types.InducedTransform, directories []string) (err error) {
	// Get list of (checked) induced transforms in topological order
	// Go through the list in reverse order and execute the luigi tasks, so the dependency tree is all set to go
	// This way we can just finish and exit with a map of the transforms to some way to track them in Luigi
	if len(transforms) != len(directories) {
		err = errors.New("Directories and Transforms don't match up")
		return
	}
	for i := 0; i < len(transforms); i++ {
		// For each InducedTransform, write it as JSON, then pass it to the Luigi task.
		// utils gives us the ProtoML directory 
		protoml_folder, err := utils.ProtoMLDir()
		exec_context := transforms[i].Exec
		if exec_context == "" {
			err := errors.New("Execution Context not specified")
			return err
		}
		if err != nil {
			return err
		}
		// Put the JSON of the induced transform into the given run folder
		logger.LogDebug(LOGTAG, "Marshaling Transform %s", directories[i])
		params, err := json.Marshal(transforms[i])
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
		//defer os.Close(log_file) this would close the file before the called process is done with it
		if err != nil {
			log_file.Close()
			return err
		}
		// Execute the Luigi Task
		// Get the path of the Luigi task
		luigi_path := path.Join(protoml_folder,"ProtoML-persist/local/fiber/TransformTask.py")
		task_add := exec.Command(luigi_path, "--run_context", exec_context, "--params_file", params_path)
		task_add.Stdout = log_file
		//task_add.Stderr = &task_err
		task_add.Start()
	}
	return
}
