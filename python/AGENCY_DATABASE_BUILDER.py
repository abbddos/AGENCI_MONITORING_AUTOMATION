# This script creates the database where AGENCI data is going to be stored...
# after conducting quality check on it, and where AGENCI's PMF is going to ...
# be updated. 
#
# This script is going to be run only once while creating the AGENCI database...
# and the initial tables needed to store the project's PMF.
#
#=======================================================================
# PLEASE DO NOT RUN THIS SCRIPT MORE THAN ONCE, OTHERWISE ALL STORED ...
# DATA WILL BE WHIPED OUT.
#=======================================================================

import sqlite3
import pandas as pd 
import json
import os

#AGENCI_DATABASE_CONNECTION = os.environ['AGENCI_DB']
#AGENCI_PMF_LOCATION = os.environ['AGENCI_PMF_LOCATION']

with open('C:\\Users\\USER\\Documents\\scripts\\python\\variables.json') as creds:
    credentials = json.load(creds)

con = sqlite3.connect(credentials['AGENCI_DATABASE_CONNECTION'])
cur = con.cursor()

PMF = pd.read_excel(credentials['AGENCI_PMF_LOCATION'], sheet_name = 'PMF')
INDICATORS_BY_LOCATION = pd.read_excel(credentials['AGENCI_PMF_LOCATION'], sheet_name = 'indicators by location')
INDICATORS_BY_RESIDENCY = pd.read_excel(credentials['AGENCI_PMF_LOCATION'], sheet_name = 'indicators by residency')

PMF.to_sql('AGENCI_OUTPUTS_PMF', con = con, if_exists ='replace')
INDICATORS_BY_LOCATION.to_sql('INDICATORS_BY_LOCATION', con = con, if_exists='replace')
INDICATORS_BY_RESIDENCY.to_sql('INDICATORS_BY_RESIDENCY', con = con, if_exists='replace')

con.close()