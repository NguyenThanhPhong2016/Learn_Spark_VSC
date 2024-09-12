

print('hello')
import sys 
import json 

args = sys.argv
print( "args : " , args )
x = json.loads( args[1] )
print( "json args : " , x , type(x) ) 

# '[\"hello\", \"world\"]'
# $Env:SRC_BASE_DIR = "data/retail_db" 
# $Env:TGT_BASE_DIR = "data/retail_db_json" 
