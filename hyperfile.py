from __future__ import print_function
import argparse
import sys 
import textwrap
import pandas as pd
from pathlib import Path
import datetime
from tableauhyperapi import Connection, HyperProcess, SqlType, TableDefinition, \
    escape_string_literal, escape_name, NOT_NULLABLE, Telemetry, Inserter, CreateMode, TableName, HyperException
n=3
# ----------------------------------------------------------------------------------
    #Import data file - filtering the inital data file
#input_data_csv = './Hyperfile_test.csv'
#df = pd.read_csv(input_data_csv)
    #Filter columns
#filter_columns = ['Operatorname','Surveycompany','Leasename','Wellnumber',
#                  'Apinumber','County','Latitude','Longitude']
#df=df[filter_columns]
#df=df.fillna(0)
# ----------------------------------------------------------------------------------

hyper_file_path = "Amazon Data.twb"    
Original_path = "D:\DavidGibsonDropbox\Part3\Amazon Data.twb"
csv_file_name = "Hyperfile_test.csv"
surveycompany_to_delete = '24'
# ----------------------------------------------------------------------------------
# organizing data
example_table = TableDefinition(
    table_name = 'Company data',

    columns = [            
            TableDefinition.Column('Apinumber', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('District', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('County', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('Wellbore profile', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('Filingpurpose', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('Amended', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('totaldepth', SqlType.big_int(), NOT_NULLABLE),
            TableDefinition.Column('Currentqueue', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('Operator Name', SqlType.text(), NOT_NULLABLE),
            TableDefinition.Column('Approvedate', SqlType.date(),NOT_NULLABLE),
            TableDefinition.Column('Updatedate', SqlType.date(), NOT_NULLABLE),
            TableDefinition.Column('Submittedate', SqlType.timestamp(),NOT_NULLABLE),
            TableDefinition.Column('Leasename', SqlType.text(), NOT_NULLABLE),
            TableDefinition.Column('Wellnumber', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('Latitude', SqlType.double(), NOT_NULLABLE),
            TableDefinition.Column('Longitude', SqlType.double(),NOT_NULLABLE),
            TableDefinition.Column('State', SqlType.text(), NOT_NULLABLE),
            TableDefinition.Column('Permit ID', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('Surveytype', SqlType.text(), NOT_NULLABLE),    
            TableDefinition.Column('depthin', SqlType.big_int(), NOT_NULLABLE),
            TableDefinition.Column('depthout', SqlType.big_int(), NOT_NULLABLE),
            TableDefinition.Column('Surveylabel', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('Othermarks', SqlType.text(), NOT_NULLABLE),
            TableDefinition.Column('Surveycompany', SqlType.text(),NOT_NULLABLE),
            TableDefinition.Column('certifcatedate', SqlType.date(), NOT_NULLABLE),           
            TableDefinition.Column('Surveystart', SqlType.date(), NOT_NULLABLE),            
            TableDefinition.Column('Surveyend', SqlType.date(), NOT_NULLABLE),
            TableDefinition.Column('Filedate', SqlType.date(), NOT_NULLABLE),            
            TableDefinition.Column('Externalattachement', SqlType.text(), NOT_NULLABLE),
            TableDefinition.Column('drillperday', SqlType.text(), NOT_NULLABLE),            
            TableDefinition.Column('Externalattachement', SqlType.text(), NOT_NULLABLE),
            TableDefinition.Column('Rigname', SqlType.text(), NOT_NULLABLE),            
            ]
    )
#-----------------------------------------------------------------------------------
# help
def parseArguments():
    parser = argparse.ArgumentParser(description = 'A simple demonstration of the Tableau SDK.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # (NOTE:'-h' and '--help' are defined by default in the ArgemuntParser
    parser.add_argument('-b','--build',actions='store_true',#default = false,
                        help = textwrap.dedent('''\
                            If an extraxt named FILENAME exists i the surrcet directory
                            extend it with sample data.
                            If no Tableau extract named FILENAME exists inthe surrect directory,
                            create one and populates it witha  sample data.
                            (default=%(default)s)
                            '''))
    parser.add_argument('-s','--spatial',action='store_true',
                        help = textwrap.dedent('''\
                        Include spatial data when creating a new extraxt."
                        If an extract is being extended, this argument is ignored/"
                        (default=%(default)s)
                        '''))
    parser.add_argument('-f','--filename',action = 'store',metavar='FILENAME',default = 'order-py.hyper',
                        help = textwrap.dedent('''\
                        FILENAME of the extract to be created or extended
                        (default=%(default)s)
                        '''))
    return vars(parser.parse_args())
#-----------------------------------------------------------------------------------
# inserting data
def hyper_insert():
    print('start')
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            print("The HyperProcess has started.")
            connection_parameters = {"lc_time":"en_US"}

            # connects to the hyper file
            with Connection(hyper.endpoint,
                   Original_path) as connection:

                print("The connection to the Hyper file is open.") 
                data =  connection.execute_list_query(f"SELECT * FROM {TableName('Extract','Extract')}")

                # connection command that runs a query
 
                with Inserter(connection,TableName('Extract','Extract')) as inserter:
                    print(inserter)
                    # try inserter
                    # 33 columns


                    inserter.add_row(['0',# API number
                                      '1',# District
                                      '2',# County
                                      '3',# Wellbore profile
                                      '4',# Filingpurpose
                                      '5',# Amended
                                      6,  # Total Depth
                                      '7',# Currentqueue
                                      '8',# Operator Name
                                      '9',# Operator Number
                                      datetime.date(2021,6,20),# Approvedate
                                      datetime.datetime(2011,11,4, 00,5,23),# Updatedate
                                      datetime.datetime(2011,11,4, 00,5,23),# Submittedate
                                      '13', # Leasename
                                      '14', # Wellnumber
                                      0.0, # Latitude
                                      0.0, # Longitude
                                      '17',# State
                                      18, # Permit ID
                                      '19', # Surveytype
                                      20, # depthin
                                      21, # depthout
                                      '22', # Surveylabel
                                      '23', # Othermarks
                                      '24', # Surveycompany
                                      datetime.date(2021,6,20), # certifcatedate
                                      datetime.date(2021,6,20), # Surveystart
                                      datetime.date(2021,6,20), # Surveyend
                                      datetime.date(2021,6,20), # Filedate
                                      '29', # Externalattachement
                                      30, # drill per day
                                      '31', # Externalattachment(2)
                                      '32' # Rigname
                                      ])
                    inserter.execute()

         
            print("The connection to the Hyper extract file is closed.")
            print("The HyperProcess has shut down.")

#"Permit_Finder_Program_date_change\Data\Gibson Reports Texas 9-5-2020.twb Files\Permit_Finder_Program_date_change.hyper
# Permit_Finder_Program_date_change.hyper"
# create a hyperfile
def hyper_csv():
    print('loading data from csv')
    path_to_database = Path(hyper_file_path)
    process_parameters = {
        "log_file_max_count": "2",
        
        "log_file_size_limit": "100M"
        
        }

        
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU,parameters = process_parameters) as hyper:
        print("The HyperProcess has started.")
        connection_parameters = {"lc_time":"en_US"}


        with Connection(hyper.endpoint,
                   hyper_file_path, 
                   CreateMode.CREATE_IF_NOT_EXISTS,
                   connection_parameters) as connection:

            print("The connection to the Hyper file is open.")

            connection.catalog.create_table(example_table)
     
            print("The table is defined.")

            path_to_csv = str(Path(__file__).parent /csv_file_name)
            count_in_customer_table = connection.execute_command(
                command=f"COPY {example_table.table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL ' ', delimiter ',', header)")


            print(f"the number of rows in table {example_table.table_name} is {count_in_customer_table}.")


            print("The data was added to the table.")
        print("The connection to the Hyper extract file is closed.")
    print("The HyperProcess has shut down.")

# delete data from hyperfile
def hyper_delete():
    #start delete file 
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            print("The HyperProcess has started.")
            connection_parameters = {"lc_time":"en_US"}

            # connects to the hyper file
            with Connection(hyper.endpoint,
                   Original_path) as connection:
                print("The connection to the Hyper file is open.") 

                delete_fuction = connection.execute_command(
                    command=f"DELETE FROM {TableName('Extract','Extract')} WHERE {('Surveycompany')}='{surveycompany_to_delete}'"
                    )
                print('delete successful')




if __name__ == '__main__':
    try:
        if n==1:
            hyper_insert()
        elif n==2:
            hyper_csv()
        elif n==3:
            hyper_delete()
        else:
            print('select a function')
    except HyperException as ex:
        print(ex)
        exit(1)