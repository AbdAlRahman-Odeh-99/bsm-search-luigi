from re import M
import luigi
import subprocess
import luigi.tools.deps_tree as deps_tree

base_dir = "/home/abd/Desktop/Work/Luigi/data/bsm-search"
code_dir = '/home/abd/Desktop/Work/Luigi/code'
thisroot_dir = '/home/abd/root/root/bin'

list_of_tasks = []

mc_options =\
    {
        "mc1": { 'type': 'mc1', 'mcweight': '0.01875', 'nevents': '40000', 'njobs': 4 },
        "mc2": { 'type': 'mc2', 'mcweight': '0.0125', 'nevents': '40000', 'njobs': 4 }
    }


def generatePrepareCommand():
    return f"""
        set -x
        rm -rf {base_dir}
        mkdir -p {base_dir}
        """
#----------------------------------- Generate Operation Start -----------------------------------
def generate_data_generation(type, nevents, job_number):
    output_file =  base_dir + "/" + type + "_" + str(job_number) + ".root"
    return {'type':type,'nevents':nevents,'jobnumber':str(job_number), 'output_file':output_file}

def generate_GenerateCommand(type, nevents, jobnumber, output_file):
    return f"""
        set -x
        source {thisroot_dir}/thisroot.sh
        pwd
        python {code_dir}/generatetuple.py {type} {nevents} {output_file}
    """

class Generate(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        return Scatter(self.data['type'])
    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    def run(self):
        type = self.data['type']
        jobnumber = self.data['jobnumber']
        nevents = self.data['nevents']
        output_file = self.data['output_file']
        bashCommand = generate_GenerateCommand(type, nevents, jobnumber, output_file)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("The error is: \n",error.decode())
#----------------------------------- Generate Operation End -----------------------------------
class PrepareDirectory(luigi.Task):
    task_namespace = 'bsm-search'

    def output(self):
        return luigi.LocalTarget(base_dir)

    def run(self):
        bashCommand = generatePrepareCommand()
        process = subprocess.Popen(bashCommand, shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("The error is: \n",error.decode())

class Scatter(luigi.Task):
    task_namespace = 'bsm-search'
    data_type = luigi.Parameter()

    def requires(self):
        return PrepareDirectory()
    
    def output(self):
        output_file = base_dir+"/"+self.data_type+".json"
        return luigi.LocalTarget(output_file)
    
    def run(self):
        import json
        option = self.data_type
        output_file = base_dir+"/"+option+".json"
        if("mc" in option):
            options = mc_options
        #elif("sig" in option):
            #options = signal_options
        #elif("data" in option):
            #options = data_options
        json_object = { option:[i+1 for i in range(options[option]['njobs'])]}
        with open(output_file,'w') as outfile:
            json.dump(json_object,outfile)

#----------------------------------- Merge Root Operation Start -----------------------------------
def merge_root_data_generation(type, njobs):
    output_file =  base_dir + "/" + type + ".root"
    return {'type':type,'njobs':njobs, 'output_file':output_file}

def merge_root_GenerateCommand(type, njobs):
    return f"""
        set -x
        BASE_DIR={base_dir}
        BASE={type}
        END={njobs}
        INPUTS=''
        OUTPUT="$BASE_DIR/$BASE"
        echo $INPUTS
        echo Output: $OUTPUT
        C=1   
        while [ $C -le $((END)) ]; do INPUTS="$INPUTS $OUTPUT""_$C.root"; C=$((C+1)); done
        echo Inputs: $INPUTS
        source {thisroot_dir}/thisroot.sh
        hadd -f $OUTPUT.root $INPUTS
        """
    #while [ $C -le $((END)) ]; do INPUTS="$INPUTS $BASE_DIR/$BASE_$C.root"; ((C++)); done
    #while [ $C -le $((END)) ]; do INPUTS="$INPUTS $BASE_DIR/$BASE_$C.root"; $((C++)); done
class Merge_Root(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        generate_list = []
        type = self.data['type']
        njobs = mc_options[type]['njobs']
        nevents = mc_options[type]['nevents']
        for i in range (0, njobs):
            data = generate_data_generation(type, nevents, i+1)
            generate_list.append(Generate(data))
        return generate_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        type = self.data['type']
        njobs = self.data['njobs']
        bashCommand = merge_root_GenerateCommand(type, njobs)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("The error is: \n",error.decode())

#----------------------------------- Merge Root Operation End -----------------------------------

if __name__ == '__main__':
    #luigi.run(['bsm-search.PrepareDirectory', '--workers', '1', '--local-scheduler'])
    #luigi.run(['bsm-search.Scatter', '--workers', '1', '--local-scheduler'])
    #luigi.build(list_of_tasks, workers=1, local_scheduler=True)
    

    '''
    for key in mc_options.keys():
        njobs = mc_options[key]['njobs']
        nevents = mc_options[key]['nevents']
        for i in range (0, njobs):
            data = generate_data_generation(key, nevents, i+1)
            list_of_tasks.append(Generate(data))
            #print(deps_tree.print_tree(Generate(data)))
    '''
    for key in mc_options.keys():
        njobs = mc_options[key]['njobs']
        data = merge_root_data_generation(key, njobs)
        list_of_tasks.append(Merge_Root(data))

    luigi.build(list_of_tasks, workers = 4)
    
    #luigi.build(AllTasks)

    

    
