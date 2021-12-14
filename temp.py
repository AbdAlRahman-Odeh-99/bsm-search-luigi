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

def select_data_genertion(type, njobs, suffix, region, variation):
    return{
        'merge_root_data': {'type': type, 'njobs': njobs},
        'input_file': type+'.root',
        'output_file': type+'_'+suffix+'.root',
        'region': region,
        'variation': variation
    }

for key in mc_options.keys():
    for option in select_mc_options:
        njobs = mc_options[key]['njobs']
        data = select_data_genertion(key, njobs, option['suffix'], option['region'], option['variation'])
        print('data in loop')
        print(data)