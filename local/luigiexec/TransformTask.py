#!/usr/bin/env python
import subprocess
import json
import luigi

class InputFile(luigi.ExternalTask):
	ifile = luigi.Parameter() # Absolute path to file needed

	def output(self):
		return luigi.LocalTarget(self.ifile)


class TransformTask(luigi.Task):
	#TODO: Should we prepend "./", or assume that's already been done?
	run_context = luigi.Parameter(description="Execution file to run. Should be executable by exec call, no interpreters assumed")
	params_file = luigi.Parameter(description="Input JSON file of the system parameters to be passed to the model. Requires inputs and outputs to be defined")
	outputs_file = "%s_outputs" % params_file
	params = json.load(open(params_file,'r'))

	def requires(self):
		return [InputFile(i['Path']) for i in params['Inputs']]
	
	def output(self):
		return luigi.LocalTarget(outputs_file)
	
	def run(self):
		retcode = subprocess.call([run_context,params_file])
		outparams_file = open(outputs_file, 'w')
		json.dump(params['Outputs'],outparams_file)
		outparams_file.close()
		return retcode

			
if __name__ == '__main__':
	# Run with python ./TransformTask.py --run_context <params['execution context']> --params_file <sysparams.json>
	luigi.run(main_task_cls=TransformTask)
