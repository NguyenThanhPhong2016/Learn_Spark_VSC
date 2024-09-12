import glob 
import re
import json
import pandas as pd 
import os
import sys 

# data_column is read by a file .json
def get_column_names(data_column, name_object):
    set_column = data_column[name_object] 
    set_column = sorted( set_column , key = lambda col: col['column_position'] )
    return [ column['column_name'] for column in set_column ]

def process_file(source_path , target_path, set_tables = None):
    # read file json from source, which include columns  
    path_data_schemas = source_path + '/schemas.json' 
    json_data_schemas = json.load( open(path_data_schemas) )
    # print(json_data_schemas)
    
    for name_table in glob.glob(f'{target_path}/*'): 
        os.remove(name_table) 
    
    # for name_table in json_data_schemas.keys():
    #     if os.path.exists(f'{target_path}/{name_table}.json'):
    #         os.remove(f'{target_path}/{name_table}.json') 
    if os.path.exists(f'{target_path}'):
        os.rmdir(f'{target_path}')
    os.makedirs(f'{target_path}',exist_ok= True)
    # return 
    
    if set_tables == None:
        set_tables = json_data_schemas.keys() 
    
    for name_table in set_tables:
        print('processing ' , name_table )
        # print(name_table)
        table_files = glob.glob(f'{source_path}/{name_table}/*') 
        for table_file in table_files: 
            df_table_data = pd.read_csv(table_file, names = get_column_names(json_data_schemas,name_table) ) 
            # print(df_table_data)
            
            #json_table have type of string. lines = true -> \n 
            json_table = df_table_data.to_json(f'{target_path}/{name_table}.json', orient = 'records' , lines= True) 
            continue
            
            # json_table = df_table_data.to_json( orient= 'records' , lines= True) 
            # print('json_table  : ' , type(json_table))
            # print(json_table)
            # with open(f'{target_path}/{name_table}.json', "w") as json_file:
            #     json_file.write( json_table)
            #     json.dump(json_table ,json_file , indent= 4)

        #os.makedirs(f'{target}/{name_table}' )

    for name_table in set_tables:
        print('processing ' , name_table )
        # print(name_table)
        df = pd.read_json(f'{target_path}/{name_table}.json', lines=True)
        print(df.head(10) )
    
if __name__ == "__main__":
    # source = 'data/retail_db' 
    # target = 'data/retail_db_json'
    
    source_path = os.environ.get('SRC_BASE_DIR') 
    target_path = os.environ.get('TGT_BASE_DIR') 
    
    if len( sys.argv ) == 2 : 
        set_tables = json.loads( sys.argv[1] ) 
        process_file( source_path, target_path, set_tables) 
    else:  
        process_file( source_path, target_path) 
# python app.py
# python app.py '[\"orders\" , \"order_items\"]' 



