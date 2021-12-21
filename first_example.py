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

select_mc_options = [
    { 'region': 'signal', 'variation': 'shape_conv_up', 'suffix': 'shape_conv_up' },
    { 'region': 'signal', 'variation': 'shape_conv_dn', 'suffix': 'shape_conv_dn' },
    { 'region': 'signal', 'variation': 'nominal,weight_var1_up,weight_var1_dn', 'suffix': 'nominal' }
]

hist_shape_mc_options = [
    { 'shapevar': 'shape_conv_up' },
    { 'shapevar': 'shape_conv_dn' }
]

variations_hist_shape_mc_options = \
    {
        'mc1': 'nominal',
        'mc2': 'nominal'
    }
  
hist_weight_mc_options = \
    {
        'mc1' : ['nominal'],
        'mc2' : ['nominal']
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

#----------------------------------- Select Operation Start -----------------------------------
def select_data_genertion(type, njobs, suffix, region, variation):
    return{
        'merge_root_data': {'type': type, 'njobs': njobs},
        'input_file': base_dir + '/' + type + '.root',
        'output_file': base_dir + '/' + type+'_'+suffix+'.root',
        'region': region,
        'variation': variation
    }

def select_GenerateCommand(input_file, output_file, region, variation):
    return f"""
        set -x
            source {thisroot_dir}/thisroot.sh        
            python {code_dir}/select.py {input_file} {output_file} {region} {variation}
        """

class Select(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        data = merge_root_data_generation(self.data['merge_root_data']['type'],self.data['merge_root_data']['njobs'])
        return Merge_Root(data)

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        input_file = self.data['input_file']
        output_file = self.data['output_file']
        region = self.data['region']
        variation = self.data['variation']
        bashCommand = select_GenerateCommand(input_file, output_file,region,variation)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("The error is: \n",error.decode())
#----------------------------------- Select Operation End -----------------------------------

#----------------------------------- Hist Shape Operation Start -----------------------------------
def hist_shape_data_genertion(type, njobs,shapevar, weight, variations):
    return {
        'input_file': base_dir + '/' + type + '_' + shapevar + '.root',
        'output_file': base_dir + '/' + type+'_'+shapevar+'_hist.root',
        'type':type,
        'njobs':njobs,
        'weight':weight,
        'shapevar':shapevar,
        'variations':variations,
        }

def hist_shape_GenerateCommand(type, input_file, output_file, shapevar, weight,variations):
    return f"""
        set -x
        source {thisroot_dir}/thisroot.sh        
        variations=$(echo {variations}|sed 's| |,|g')
        name="{type}_{shapevar}"
        python {code_dir}/histogram.py {input_file} {output_file} {type} {weight} {variations} $name
        """

class Hist_Shape(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        select_list = []
        type = self.data['type']
        for option in select_mc_options:
            njobs = self.data['njobs']
            data = select_data_genertion(type, njobs, option['suffix'], option['region'], option['variation'])
            select_list.append(Select(data))
        return select_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        type = self.data['type']
        input_file = self.data['input_file']
        output_file = self.data['output_file']
        weight = self.data['weight']
        variations = self.data['variations']
        shapevar = self.data['shapevar']
        bashCommand = hist_shape_GenerateCommand(type, input_file, output_file,shapevar, weight,variations)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("The error is: \n",error.decode())
#----------------------------------- Hist Shape Operation End -----------------------------------

#----------------------------------- Hist Weight Operation Start -----------------------------------
def hist_weight_data_genertion(type, njobs,shapevar, weight, variations):
    input_file = ''
    output_file = type+'_hist.root'
    if('mc' in type or 'sig' in type):
        input_file = type+'_'+shapevar+'.root'
        output_file = type+'_'+shapevar+'_hist.root'
    return {
        'input_file': base_dir + '/' + input_file,
        'output_file': base_dir + '/' + output_file,
        'type':type,
        'njobs':njobs,
        'weight':weight,
        'variations':variations
        }

def hist_weight_GenerateCommand(type, input_file, output_file, weight,variations):
    return f"""
        set -x
        source {thisroot_dir}/thisroot.sh        
        variations=$(echo {variations}|sed 's| |,|g')
        name={type}
        python {code_dir}/histogram.py {input_file} {output_file} $name {weight} {variations}
    """
    
class Hist_Weight(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        select_list = []
        type = self.data['type']
        for option in select_mc_options:
            njobs = self.data['njobs']
            data = select_data_genertion(type, njobs, option['suffix'], option['region'], option['variation'])
            select_list.append(Select(data))
        return select_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        type = self.data['type']
        input_file = self.data['input_file']
        output_file = self.data['output_file']
        weight = self.data['weight']
        variations = self.data['variations']
        bashCommand = hist_weight_GenerateCommand(type, input_file, output_file, weight, variations)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("The error is: \n",error.decode())
#----------------------------------- Hist Weight Operation End -----------------------------------


#----------------------------------- Merge Explicit Operation Start -----------------------------------
def merge_explicit_data_genertion(option, operation, variations): #, njobs, weight, ):
    type = option['type']
    input_files = ''
    output_file = type+'_merged_hist.root'

    if('mc' in type):
        if('merge_hist_shape' in operation):
            input_files = type+'_shape_conv_up_hist.root '+type+'_shape_conv_dn_hist.root'
            output_file = type+'_shape_hist.root'
        elif('merge_hist_all' in operation):
            input_files = type+'_nominal_hist.root '+type+'_shape_hist.root'
    elif('sig' in type):
        input_files = type+'_nominal_hist.root'
    elif('data' in type):
        input_files = type+"_hist.root qcd_hist.root"
    elif('all' in type):
        input_files = "mc1_merged_hist.root mc2_merged_hist.root sig_merged_hist.root data_merged_hist.root"
        output_file = "all_merged_hist.root"      
    return{
        'option': option,
        'input_files': input_files,
        'operation' : operation,
        'output_file': base_dir + "/" + output_file,
        'variations' : variations
    }

def merge_explicit_GenerateCommand(input_files, output_file):
    return f"""
        set -x
        input_files=${input_files}
        INPUTS=""
        for i in {input_files}; do
        INPUTS="$INPUTS $(printf {base_dir}/$i)"
        done
        source {thisroot_dir}/thisroot.sh        
        hadd -f {output_file} $INPUTS
    """

class Merge_Explicit(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        dependency_list = []
        option = self.data['option']
        type = option['type']
        njobs = option['njobs']
        weight = option['mcweight']
        operation = self.data['operation']
        variations = self.data['variations']
        if ('merge_hist_all' in operation):
            data = merge_explicit_data_genertion(option, 'merge_hist_shape', variations)
            dependency_list.append(Merge_Explicit(data))
            for weight_option in hist_weight_mc_options[type]:
                data = hist_weight_data_genertion(type, njobs, weight_option, weight, variations)
                dependency_list.append(Hist_Weight(data))
        else:
            for shape_option in hist_shape_mc_options:
                data = hist_shape_data_genertion(type, njobs, shape_option['shapevar'], weight, variations)
                dependency_list.append(Hist_Shape(data))

        return dependency_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        input_files = self.data['input_files']
        output_file = self.data['output_file']   
        bashCommand = merge_explicit_GenerateCommand(input_files, output_file)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("The error is: \n",error.decode())
#----------------------------------- Merge Explicit Operation End -----------------------------------




if __name__ == '__main__':
    #luigi.run(['bsm-search.PrepareDirectory', '--workers', '1', '--local-scheduler'])
    #luigi.run(['bsm-search.Scatter', '--workers', '1', '--local-scheduler'])
    #luigi.build(list_of_tasks, workers=1, local_scheduler=True)
    
    '''
    def requires(self):
        select_list = []
        type = self.data['type']
        for option in select_mc_options:
            njobs = self.data['njobs']
            data = select_data_genertion(typ
    '''
    '''
    for key in mc_options.keys():
        njobs = mc_options[key]['njobs']
        data = merge_root_data_generation(key, njobs)
        list_of_tasks.append(Merge_Root(data))
    '''
    '''
    for key in mc_options.keys():
        for option in select_mc_options:
            njobs = mc_options[key]['njobs']
            data = select_data_genertion(key, njobs, option['suffix'], option['region'], option['variation'])
            list_of_tasks.append(Select(data))
    '''
    '''
    for key in mc_options.keys():
        njobs = mc_options[key]['njobs']
        weight = mc_options[key]['mcweight']
        variations = variations_hist_shape_mc_options[key]
        for option in hist_shape_mc_options:    
            data = hist_shape_data_genertion(key, njobs, option['shapevar'], weight, variations)
            list_of_tasks.append(Hist_Shape(data))
        for option in hist_weight_mc_options[key]:
            data = hist_weight_data_genertion(key, njobs, option, weight, variations)
            list_of_tasks.append(Hist_Weight(data))
    '''
    '''
    for key in mc_options.keys():
        option = mc_options[key]
        type = option['type']
        njobs = option['njobs']
        weight = option['mcweight']
        variations = variations_hist_shape_mc_options[type]
        data = merge_explicit_data_genertion(option, 'merge_hist_shape')
        list_of_tasks.append(Merge_Explicit(data))
        for weight_option in hist_weight_mc_options[type]:
            data = hist_weight_data_genertion(type, njobs, weight_option, weight, variations)
            list_of_tasks.append(Hist_Weight(data))
    '''
    for key in mc_options.keys():
        option = mc_options[key]
        type = option['type']
        njobs = option['njobs']
        weight = option['mcweight']
        variations = variations_hist_shape_mc_options[type]
        data = merge_explicit_data_genertion(option, 'merge_hist_all', variations)
        list_of_tasks.append(Merge_Explicit(data))
        
    
    luigi.build(list_of_tasks, workers = 4)
    
    #luigi.build(AllTasks)