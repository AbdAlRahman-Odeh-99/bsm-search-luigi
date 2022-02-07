from re import M
import luigi
import subprocess
import luigi.tools.deps_tree as deps_tree
from sqlalchemy import null

base_dir = "/home/abd/Desktop/Work/Luigi/data/bsm-search"
code_dir = '/home/abd/Desktop/Work/Luigi/code'
thisroot_dir = '/home/abd/root/root/bin'

list_of_tasks = []

#----------------------------------- MC CONFIGURATIONS START -----------------------------------
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

hist_weight_variations_options = \
    {
        'mc1': 'nominal',
        'mc2': 'nominal',
        'sig': 'nominal',
        'data' : 'nominal'
    }
  
hist_weight_shapevar_options = \
    {
        'mc1' : 'nominal',
        'mc2' : 'nominal',
        'sig' : 'nominal',
        'data' : 'nominal'
    }
#----------------------------------- MC CONFIGURATIONS END -----------------------------------

#----------------------------------- SIGNAL CONFIGURATIONS START ----------------------------------- 
signal_options = \
    {
        'sig' : { 'type': 'sig', 'mcweight': '0.0025', 'nevents': '40000', 'njobs': 2 }
    }

select_signal_options = [
    {'region':'signal','variation':'nominal','suffix':'nominal'}
    ]
#----------------------------------- SIGNAL CONFIGURATIONS END ----------------------------------- 

#----------------------------------- DATA CONFIGURATIONS START ----------------------------------- 
data_options = \
    {
        'data' : { 'type': 'data', 'nevents': '20000', 'njobs': 5 }
    }

select_data_options = \
    [
        { 'region': 'signal', 'variation': 'nominal', 'suffix': 'signal' },
        { 'region': 'control', 'variation': 'nominal', 'suffix': 'control' }
    ]

hist_weight_data_options = \
    {
        'data': { 'parent_type':'data', 'sub_type': 'signal', 'result':'data', 'weight': 1.0 },
        'qcd': { 'parent_type':'data', 'sub_type': 'control', 'result': 'qcd', 'weight': 0.1875 }
    }
#----------------------------------- DATA CONFIGURATIONS END ----------------------------------- 


def generatePrepareCommand():
    return f"""
        rm -rf {base_dir}
        mkdir -p {base_dir}
        """
#----------------------------------- Generate Operation Start -----------------------------------
def generate_data_generation(option, job_number):
    type = option['type']
    output_file =  base_dir + "/" + type + "_" + str(job_number) + ".root"
    return {
        'option':option,
        'jobnumber':str(job_number), 
        'output_file':output_file
    }

def generate_GenerateCommand(data):
    return f"""
        
        source {thisroot_dir}/thisroot.sh
        pwd
        python {code_dir}/generatetuple.py {data['option']['type']} {data['option']['nevents']} {data['output_file']}
    """

class Generate(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        return Scatter(self.data['option'])
    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    def run(self):
        bashCommand = generate_GenerateCommand(self.data)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("Return Code:", process.returncode)
        if process.returncode and error:
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
        print("Return Code:", process.returncode)
        if process.returncode and error:
            print("The error is: \n",error.decode())

class Scatter(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        return PrepareDirectory()
    
    def output(self):
        output_file = base_dir+"/"+self.data['type']+".json"
        return luigi.LocalTarget(output_file)
    
    def run(self):
        import json
        option = self.data
        type = option['type']
        output_file = base_dir+"/"+type+".json"

        json_object = { type:[i+1 for i in range(option['njobs'])]}
        with open(output_file,'w') as outfile:
            json.dump(json_object,outfile)
#----------------------------------- Merge Root Operation Start -----------------------------------
#def merge_root_data_generation(type, njobs):
def merge_root_data_generation(option):
    type = option['type']
    njobs = option['njobs']
    nevents = option['nevents']
    output_file = base_dir + '/' + type + '.root'
    input_files = ''
    for i in range (1,njobs+1):
        input_files += ' ' + base_dir + '/' + type + '_' + str(i) + '.root'
    return {
        'option':option, 
        'output_file':output_file, 
        'input_files':input_files
    }

def merge_root_GenerateCommand(data):
    return f"""

        source {thisroot_dir}/thisroot.sh
        hadd -f {data['output_file']} {data['input_files']}
        """
    #while [ $C -le $((END)) ]; do INPUTS="$INPUTS $BASE_DIR/$BASE_$C.root"; ((C++)); done
    #while [ $C -le $((END)) ]; do INPUTS="$INPUTS $BASE_DIR/$BASE_$C.root"; $((C++)); done

class Merge_Root(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        generate_list = []
        option = self.data['option'] 
        type = option['type']
        njobs = option['njobs']
        nevents = option['nevents']
        for i in range (0, njobs):
            data = generate_data_generation(option, i+1)
            generate_list.append(Generate(data))
        return generate_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        bashCommand = merge_root_GenerateCommand(self.data)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("Return Code:", process.returncode)
        if process.returncode and error:
            print("The error is: \n",error.decode())
#----------------------------------- Merge Root Operation End -----------------------------------

#----------------------------------- Select Operation Start -----------------------------------
#def select_data_genertion(type, njobs, suffix, region, variation):
def select_data_genertion(option, select_option):
    type = option['type']
    njobs = option['njobs']
    nevents = option['nevents']
    print('checkpoint')
    print(select_option)
    suffix = select_option['suffix']
    region =  select_option['region']
    variation = select_option['variation']
    return{
        'merge_root_data': {'type': type, 'njobs': njobs, 'nevents': nevents},
        'input_file': base_dir + '/' + type + '.root',
        'output_file': base_dir + '/' + type+'_'+suffix+'.root',
        'region': region,
        'variation': variation
    }

def select_GenerateCommand(data):
    return f"""
        
            source {thisroot_dir}/thisroot.sh        
            python {code_dir}/select.py {data['input_file']} {data['output_file']} {data['region']} {data['variation']}
        """

class Select(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        data = merge_root_data_generation(self.data['merge_root_data'])
        return Merge_Root(data)

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        bashCommand = select_GenerateCommand(self.data)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("Return Code:", process.returncode)
        if process.returncode and error:
            print("The error is: \n",error.decode())
#----------------------------------- Select Operation End -----------------------------------

#----------------------------------- Hist Shape Operation Start -----------------------------------
def hist_shape_data_genertion(option, shapevar, variations):
    type = option['type']
    return {
        'input_file': base_dir + '/' + type + '_' + shapevar + '.root',
        'output_file': base_dir + '/' + type+'_'+shapevar+'_hist.root',
        'option':option,
        'shapevar':shapevar,
        'variations':variations,
        "name":type+"_"+shapevar
        }

def hist_shape_GenerateCommand(data):
    return f"""
        
        source {thisroot_dir}/thisroot.sh        
        python {code_dir}/histogram.py {data['input_file']} {data['output_file']} {data['option']['type']} {data['option']['mcweight']} {data['variations']} {data['name']}
        """

class Hist_Shape(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        select_list = []
        option = self.data['option']
        type = option['type']
        select_options = null
        if ('mc' in type):
            select_options = select_mc_options
        elif ('sig' in type):
            select_options = select_signal_options
        for select_option in select_options:
            data = select_data_genertion(option, select_option)
            select_list.append(Select(data))
        return select_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        bashCommand = hist_shape_GenerateCommand(self.data)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("Return Code:", process.returncode)
        if process.returncode and error:
            print("The error is: \n",error.decode())
#----------------------------------- Hist Shape Operation End -----------------------------------

#----------------------------------- Hist Weight Operation Start -----------------------------------
def hist_weight_data_genertion(option, shapevar, variations, hist_weight_data_options = null):
    type = option['type']
    weight = null
    input_file = ''
    output_file = type+'_hist.root'
    if('mc' in type or 'sig' in type):
        input_file = type+'_'+shapevar+'.root'
        output_file = type+'_'+shapevar+'_hist.root'
        weight = option['mcweight']
    elif ('data' in type and hist_weight_data_options != null):
        input_file = type+'_'+hist_weight_data_options['sub_type']+'.root'
        output_file = hist_weight_data_options['result']+'_hist.root'
        weight = hist_weight_data_options['weight']
        if('control' in hist_weight_data_options['sub_type']):
            type = hist_weight_data_options['result']
    if('sig' in type):
        type = type+'nal'
    return {
        'input_file': base_dir + '/' + input_file,
        'output_file': base_dir + '/' + output_file,
        'option':option,
        'weight':weight,
        'variations':variations,
        'name':type
        }

def hist_weight_GenerateCommand(data):
    return f"""
        source {thisroot_dir}/thisroot.sh        
        python {code_dir}/histogram.py {data['input_file']} {data['output_file']} {data['name']} {data['weight']} {data['variations']}
    """
    
class Hist_Weight(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        select_list = []
        option = self.data['option']
        type = option['type']
        select_options = null
        if ('mc' in type):
            select_options = select_mc_options
        elif ('sig' in type):
            select_options = select_signal_options
        elif ('data' in type):
            select_options = select_data_options

        for select_option in select_options:
            data = select_data_genertion(option, select_option)
            select_list.append(Select(data))
        return select_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self):
        bashCommand = hist_weight_GenerateCommand(self.data)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("Return Code:", process.returncode)
        if process.returncode and error:
            print("The error is: \n",error.decode())
#----------------------------------- Hist Weight Operation End -----------------------------------

#----------------------------------- Merge Explicit Operation Start -----------------------------------
def merge_explicit_data_genertion(option, operation, variations): #, njobs, weight, ):
    type = option['type']
    input_files = ''
    output_file = type+'_merged_hist.root'

    if('mc' in type):
        if('merge_hist_shape' in operation):
            #input_files = base_dir + '/' + type + '_shape_conv_up_hist.root ' + \
            input_files = base_dir + '/' + type + '_shape_conv_up_hist.root ' + \
                          base_dir + '/' + type +'_shape_conv_dn_hist.root'
            output_file = type+'_shape_hist.root'
        elif('merge_hist_all' in operation):
            input_files = base_dir + '/' + type + '_nominal_hist.root ' + \
                          base_dir + '/' + type + '_shape_hist.root'
    elif('sig' in type):
        input_files = base_dir + '/' + type + '_nominal_hist.root'
    elif('data' in type):
        input_files = base_dir + '/' + type + '_hist.root '+ base_dir+'/qcd_hist.root'
    elif('all' in type):
        input_files = base_dir + '/' + 'mc1_merged_hist.root ' + \
                      base_dir + '/' + 'mc2_merged_hist.root ' + \
                      base_dir + '/' + 'sig_merged_hist.root ' + \
                      base_dir + '/' + 'data_merged_hist.root'
        output_file = "all_merged_hist.root"

    return{
        'option': option,
        'input_files': input_files,
        'operation' : operation,
        'output_file': base_dir + "/" + output_file,
        'variations' : variations
    }

def merge_explicit_GenerateCommand(data):
    return f"""
        
        source {thisroot_dir}/thisroot.sh        
        hadd -f {data['output_file']} {data['input_files']}
    """

class Merge_Explicit(luigi.Task):
    task_namespace = 'bsm-search'
    data = luigi.DictParameter()

    def requires(self):
        dependency_list = []
        option = self.data['option']
        type = option['type']
        operation = self.data['operation']
        variations = self.data['variations']
        if ('merge_hist_all' in operation):
            if('mc' in type):
                data = merge_explicit_data_genertion(option, 'merge_hist_shape', variations)
                dependency_list.append(Merge_Explicit(data))

            shapevar = hist_weight_shapevar_options[type]
            
            if('data' in type):
                for hist_weight_key in hist_weight_data_options.keys():
                    hist_weight_option = hist_weight_data_options[hist_weight_key]
                    data = hist_weight_data_genertion(option, shapevar, variations, hist_weight_option)
                    dependency_list.append(Hist_Weight(data))
            else:
                data = hist_weight_data_genertion(option, shapevar, variations)
                dependency_list.append(Hist_Weight(data))
        else:
            print('inside else')
            for shape_option in hist_shape_mc_options:
                data = hist_shape_data_genertion(option, shape_option['shapevar'], variations)
                dependency_list.append(Hist_Shape(data))

        return dependency_list

    def output(self):
        output_file = self.data['output_file']
        return luigi.LocalTarget(output_file)
    
    def run(self): 
        bashCommand = merge_explicit_GenerateCommand(self.data)
        process = subprocess.Popen(bashCommand, shell = True, executable='/bin/bash',stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        output, error = process.communicate()
        print("The command is: \n",bashCommand)
        print("The output is: \n",output.decode())
        print("Return Code:", process.returncode)
        if process.returncode and error:
            print("The error is: \n",error.decode())
            exit("Return code : "+ str(process.returncode) + " \nError message: " + error.decode())
#----------------------------------- Merge Explicit Operation End -----------------------------------

if __name__ == '__main__':

    #------------------------------------ MC WORKFLOW START ------------------------------------
    for key in mc_options.keys():
        option = mc_options[key]
        type = option['type']
        njobs = option['njobs']
        weight = option['mcweight']
        variations = hist_weight_variations_options[type]
        data = merge_explicit_data_genertion(option, 'merge_hist_all', variations)
        list_of_tasks.append(Merge_Explicit(data))
    #------------------------------------ MC WORKFLOW END ------------------------------------
    
    #------------------------------------ SIGNAL WORKFLOW START ------------------------------------
    for key in signal_options.keys():
        option = signal_options[key]
        type = option['type']
        weight = option['mcweight']
        variation = hist_weight_variations_options[type]
        data = merge_explicit_data_genertion(option, 'merge_hist_all', variation)
        list_of_tasks.append(Merge_Explicit(data))
    #------------------------------------ SIGNAL WORKFLOW END ------------------------------------
    
    #------------------------------------ Data WORKFLOW START ------------------------------------
    for key in data_options.keys():
        option = data_options[key]    
        variations = hist_weight_variations_options[key]
        data = merge_explicit_data_genertion(option, 'merge_hist_all', variations)
        list_of_tasks.append(Merge_Explicit(data))
    #------------------------------------ Data WORKFLOW END ------------------------------------

    luigi.build(list_of_tasks, workers = 4)
    
    #luigi.build(AllTasks)