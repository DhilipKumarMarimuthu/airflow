from datetime import datetime
import numpy as np
import pandas as pd
import re
import math
import types


def calc_gor(qo, qg):
    gor = 1000 * np.divide(qg, qo)
    return gor  
    
def calc_watercut(qo, qw):
    wct = np.divide(qw, qo + qw)
    return wct

def range_validation(num, minn, maxn):
    return max(min(maxn, num), minn) if not math.isnan(num) else math.nan

def empty_check(value, parameter):
    if(not(str(value) and not str(value).isspace())):
        return missing_value_dict[parameter]
    return value

def psig_to_psia_conversion(param_value, unit_value):
    return param_value + unit_value

def mscfd_to_scfd_conversion(param_value, unit_value):
    return param_value * unit_value    

def calc_liquidrate(qo, qw):
    return np.add(qo, qw)

def unit_conversion(param_value, unit_parameter):
    unit_header_value = unit_conversion_header_dict[unit_parameter]
    unit_value = unit_conversion_value_dict[unit_header_value]
    func = unit_conversion_func_dict.get(unit_header_value, lambda: param_value)
    return func(param_value, unit_value)

def cum_calc(df, parameter):
    return df[parameter].cumsum(axis=0)

def cum_on_liquid_calc(df):
    df['time_on_liquid'] = 0
    df['cum_time_on_liquid'] = 0
    for index in range(1, len(df)):
        time_delta = pd.to_datetime(df.loc[index,'TIME']) - pd.to_datetime(df.loc[index-1,'TIME'])
        date_difference = time_delta.total_seconds() / 86400
        df.loc[index,'time_on_liquid'] = date_difference if df.loc[index,'LiquidRate'] > 0 else 0

    df['time_on_liquid'] = df['time_on_liquid'].fillna(0)
    df['cum_time_on_liquid'] = df['time_on_liquid'].cumsum(axis=0)
    return df['cum_time_on_liquid']

def split_remarks(remarks, search_text):    
    if search_text in remarks:
        split_list = re.split('=|:',remarks)
        if(len(split_list) > 0 and len(split_list) == 2):
            return int(re.search(r'\d+', split_list[1]).group()) if hasNumbers(split_list[1]) else 0
        else:
            return 0
    else:
        return 0

def split_gravity(gravity):
    split_list = re.split('=|:|&',gravity)
    if(len(split_list) > 0):
        return filter_column(split_list[0])
    else:
        return gravity

def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

def filter_column(column_name):
    return re.sub(r'[^\w\s]', '', column_name).strip()

def filter_csv_data(**kwargs):
    print(kwargs)
    print(kwargs['filepath'])
    print(kwargs['filter_columns'])
    file_path = kwargs['filepath']
    filter_path = kwargs['filterpath']
    filter_column_names = [kwargs['filter_columns']]
    df = pd.read_csv(file_path)
    for column_name in filter_column_names:
        df[column_name] = df[column_name].map(lambda x: filter_column(x))

    df.to_csv(filter_path, index=False)
    
def filter_flowback_data(**kwargs):
    file_path = kwargs['filepath']
    filter_path = kwargs['filterpath']
    filter_column_names = [kwargs['filter_columns']]
    print(file_path)
    df = pd.read_excel(
    file_path,engine='openpyxl',
    header=[10,11,12],sheet_name= "Hourly Data", index_col=None)

    well_column_filtered_list =[]
    df_filter = df.iloc[:,1:35]
    for index in range(0, len(df_filter.columns)):    
        sub_list = []
        for j in range(0, len(df_filter.columns[index])):
            if(isinstance(df_filter.columns[index][j], int)):
                sub_list.append(str(df_filter.columns[index][j]))
            else:
                sub_list.append(df_filter.columns[index][j])
        well_column_filtered_list.append('_'.join(sub_list))


    df_filter.columns = well_column_filtered_list
    start_position = 0
    column_exists = False
    column_name = None
    
    for pos in range(start_position, len(well_column_filtered_list)):
        column_name = well_column_filtered_list[pos]
        for sub_item in well_column_list:
            if sub_item in column_name:
                item_exists = True
                column_name = sub_item
                break
        if(item_exists):
            well_column_filtered_list[pos] = column_name
            item_exists = False
        else:
            well_column_filtered_list[pos] = 'Remove'
    df_filter.columns = well_column_filtered_list
    df_filter.drop("Remove", axis=1, inplace=True)
    
    df_well = pd.read_excel(
    file_path, engine="openpyxl",
    header=[4],sheet_name= "Hourly Data", usecols="G:I", index_col=None, nrows=2)
    
    df_well = df_well.loc[:, ~df_well.columns.str.contains('^Unnamed')]
    df_well = df_well.dropna()
    
    well_name = f'{df_well.columns[1]} {df_well.iloc[0,1]}'
    
    df_filter['well'] = well_name
    
    df_gravity = pd.read_excel(
    file_path,engine="openpyxl",
    header=[9],sheet_name= "Hourly Data", usecols="AI:AJ", index_col=None, nrows=0)
    gravity_value = split_gravity(df_gravity.columns[1]) if isinstance(df_gravity.columns[1],str) else df_gravity.columns[1]

    
    df_filter['Gravity'] = gravity_value
    
    for column_name in filter_column_names:
        df_filter[column_name] = df_filter[column_name].map(lambda x: filter_column(x))
        
    df_filter.to_csv(filter_path, index=None)

def validate_flowback_data(**kwargs):   
    file_path = kwargs['filepath']
    filter_path = kwargs['filterpath']
    filter_column_names = [kwargs['filter_columns']]
    print(file_path)
    df = pd.read_csv(file_path)
    df['H2S'] = 0
    df['CHL'] = 0    
    df['REMARKS'] = df['REMARKS'].fillna('')
    # Make missing, range and unit validation in one function with one for loop
    # Missing validation
    for index in range(0, len(df)):        
        df.loc[index,'Tbg Press'] = empty_check(df.loc[index,'Tbg Press'], 'psia')
        df.loc[index,'Csg Press'] = empty_check(df.loc[index,'Csg Press'], 'psia')
        df.loc[index,'MCFD'] = empty_check(df.loc[index,'MCFD'], 'rate')
        df.loc[index,'BOPH'] = empty_check(df.loc[index,'BOPH'], 'rate')
        df.loc[index,'BWPH'] = empty_check(df.loc[index,'BWPH'], 'rate')        
        df.loc[index,'H2S'] = split_remarks(df.loc[index,'REMARKS'], 'H2S')
        df.loc[index,'CHL'] = split_remarks(df.loc[index,'REMARKS'], 'CHL')
    
    # Range validation
    for index in range(0, len(df)):        
        df.loc[index,'Tbg Press'] = range_validation(df.loc[index,'Tbg Press'], 0, 20000)
        df.loc[index,'Csg Press'] = range_validation(df.loc[index,'Csg Press'], 0, 20000)
        df.loc[index,'MCFD'] = range_validation(df.loc[index,'MCFD'], 0, 100000)
        df.loc[index,'BOPH'] = range_validation(df.loc[index,'BOPH'], 0, 100000)
        df.loc[index,'BWPH'] = range_validation(df.loc[index,'BWPH'], 0, 100000)
        df.loc[index,'Total Choke'] = range_validation(df.loc[index,'Total Choke'], 0, 192)
        
    # Unit validation
    for index in range(0, len(df)):        
        df.loc[index,'Tbg Press'] = unit_conversion(df.loc[index,'Tbg Press'], 'psig')
        df.loc[index,'Csg Press'] = unit_conversion(df.loc[index,'Csg Press'], 'psig')
        df.loc[index,'MCFD'] = unit_conversion(df.loc[index,'MCFD'], 'mscfd')
        
    df.to_csv(filter_path, index=None)   
    
    
def process_flowback_data(**kwargs):   
    file_path = kwargs['filepath']
    filter_path = kwargs['filterpath']
    filter_column_names = [kwargs['filter_columns']]
    print(file_path)
    df = pd.read_csv(file_path)
    timeon_liquid_columns = ['TIME', 'LiquidRate']
    for column in calculated_column_list:
        df[column]=0
    
    for index in range(0, len(df)):        
        print(df.loc[index,'MCFD'])
        df.loc[index,'MCFH'] = df.loc[index,'MCFD']/24
        df.loc[index,'BOPD'] = df.loc[index,'BOPH'] * 24
        df.loc[index,'BWPD'] = df.loc[index,'BWPH'] * 24
        df.loc[index,'LiquidRate'] = df.loc[index,'BOPH'] + df.loc[index,'BWPH']        
        df.loc[index,'GOR'] = np.divide(df.loc[index,'MCFH'],df.loc[index,'BOPH']) if df.loc[index,'BOPH'] > 0 else 0        
        df.loc[index,'H2S'] = df.loc[index-1,'H2S'] if index > 0 and df.loc[index,'H2S'] == 0 else df.loc[index,'H2S']
        df.loc[index,'CHL'] = df.loc[index-1,'CHL'] if index > 0 and df.loc[index,'CHL'] == 0 else df.loc[index,'CHL']        
        
    df['TimeOnLiquid'] = cum_on_liquid_calc(df[timeon_liquid_column_list])
    
    for cum in cum_column_dict:
        df[cum] = cum_calc(df,cum_column_dict[cum])
    
    df.to_csv(filter_path, index=None)
    
    
missing_value_dict = {'psia' : math.nan, 'rate': 0 }
unit_conversion_header_dict = {'psig' : 'psia', 'mscfd': 'scfd' }
unit_conversion_value_dict = {'psia' : 14.7, 'scfd': 1000 }
unit_conversion_func_dict = {'psia' : psig_to_psia_conversion, 'scfd': mscfd_to_scfd_conversion }
well_column_list =['TIME','Tbg Press','Csg Press','Total Choke','MCFD','BOPH','BOPD','BWPH','BWPD','H2O Trucks','REMARKS']
calculated_column_list=['MCFH','LiquidRate','GOR', 'CumOil', 'CumWater', 'CumGas','TimeOnLiquid']
cum_column_dict = {'CumOil': 'BOPH', 'CumWater': 'BWPH', 'CumGas': 'MCFH'}
timeon_liquid_column_list = ['TIME','LiquidRate']

