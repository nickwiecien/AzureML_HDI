from azureml.core import Run, Workspace, Datastore, Dataset
import pandas as pd
import os
import argparse
import numpy as np

# Parse input arguments
parser = argparse.ArgumentParser("Get data from Azure HDInsight and register in AML workspace")
parser.add_argument('--hdi_dataset', dest='hdi_dataset', required=True)

args, _ = parser.parse_known_args()
hdi_dataset = args.hdi_dataset

# Get current run
current_run = Run.get_context()

# Get associated AML workspace
ws = current_run.experiment.workspace

# Connect to default blob datastore
ds = ws.get_default_datastore()

# Get default Key Vault
kv = ws.get_default_keyvault()

# Get HDInsight Password
password = kv.get_secret('hdipassword')
server = kv.get_secret('hdiserver')

# Query HDInsight
import jaydebeapi
import pandas as pd

driver = 'org.apache.hive.jdbc.HiveDriver'
driver_path = '~/jdbcdriver/hive-jdbc-uber-2.6.3.0-235.jar'
username = 'admin'

conn = jaydebeapi.connect(driver,
       f"jdbc:hive2://{server}:443/;ssl=true;transportMode=http;httpPath=/hive2", 
       [username, password],
       driver_path)

curs = conn.cursor()
curs.execute('select * from hivesampletable limit 50')
rows = curs.fetchall()   
df = pd.DataFrame(rows, columns=[column[0].replace('hivesampletable.','') for column in curs.description])

# Make directory on mounted storage for output dataset
os.makedirs(hdi_dataset, exist_ok=True)

# Save modified dataframe
df.to_csv(os.path.join(hdi_dataset, 'hdi_data.csv'), index=False)