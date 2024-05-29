import xmltodict, glob
import os
from tqdm import tqdm
import pg_server
from psycopg2.extras import Json

def to_postgres(item, json_result, table_name, schema_name, conn):

    cur = conn.cursor()

    sql = f'''INSERT INTO {schema_name}.{table_name}
                VALUES(%s, %s)'''

    cur.execute(sql, (item, Json(json_result)))    
    conn.commit()
    cur.close()

                
def handler(path, schema_name, dbname):
    conn  = pg_server.connect(dbname)
    all_filepaths = glob.glob(path)
    table_name = 'json_raw'

    #for each xml
    for filepath in tqdm(all_filepaths):
        with open(filepath, 'r') as fout:
            mydict = fout.read()
            xml_dictionary = xmltodict.parse(mydict)
            filename = os.path.basename(filepath)

            #write to postgres
            to_postgres(filename, xml_dictionary, table_name, schema_name, conn)        

if __name__ == '__main__':

    #location of XML Files (include slash at end of path)
    path = ''
    schema_name = ''
    handler(path, schema_name, 'dev')
  

