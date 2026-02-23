# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

!pip install psycopg2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import psycopg2
import json
import pandas as pd

connection_id = '3163bd32-2a14-4226-ba55-5d0265ce1e7a' # connection name: "AirlinesDBConn platformadmin"
connection_credential = notebookutils.connections.getCredential(connection_id)
credential_dict = json.loads(connection_credential['credential'])
user_name = next(item['value'] for item in credential_dict['credentialData'] if item['name'] == 'username')
pwd = next(item['value'] for item in credential_dict['credentialData'] if item['name'] == 'password')

server_name = 'aaaorgpgflexserver.postgres.database.azure.com'
database_name = 'airlines'

conn = psycopg2.connect(
    host=server_name,
    user=user_name,
    password=pwd,
    dbname=database_name,
    port=5432,
    sslmode="require",
)

cursor = conn.cursor()
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
    LIMIT 1;
""")

tables = list(cursor.fetchall())
table_name = tables[0][0]
df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

print('Tables:')
for table in tables:
    print(f'- {table[0]}')

print(f"\nRows in table '{table_name}':")
display(df)

cursor.close()
conn.close()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
