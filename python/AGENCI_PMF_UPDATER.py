# The following process collects the data that is quality checked...
# earlier, calculates the indicators according to AGENCI indicators...
# calculation logic and updates the project's PMF stored in ...
# AGENCI's database accordingly with its disaggregations by ...
# location and residency statuses of reached beneficiaries. 
#
# This process will take place right after the quality check process.

import numpy as np
import pandas as pd
import datetime as dt
import urllib
import json
import sqlite3
import os

# Database connection credentials, the links that lead to where the data...
# is stored and other variables that need to be protected should be stored in
# the environment variables. However, privilege required to access environment
# variables was not granted on the computer on which this algorithm was developed, 
# so we had to find another way...
# 
# A JSON file was created and all required variables were stored there, 
# the mentioned JSON file remains on the computer where the algorithm is...
# developed, and was not committed to the Github repository that contains...
# this algorithm.

with open('C:\\Users\\USER\\Documents\\scripts\\python\\variables.json') as creds:
    credentials = json.load(creds)

#AGENCI_DATABASE_CONNECTION = os.environ.get('AGENCI_DB')

# Connection with AGENCI database...
con = sqlite3.connect(credentials['AGENCI_DATABASE_CONNECTION'])
cur = con.cursor()

# Fetching PMF as updated last month.
PMF = pd.read_sql('select * from AGENCI_OUTPUTS_PMF', con)
PMF = PMF.set_index('index')
PMF = PMF.fillna(0)

# Create date for filtering and updates...
this_month = pd.to_datetime('today').month
last_month = this_month - 1
y = pd.to_datetime('today').year
dd = str(last_month) + '-' + str(y)
dd = dt.date(y,last_month, 1).strftime('%b-%y')

# Get indicators structure...
# AGENCI output indicators were put in 3 structures to be...
# monitored; the PMF itself, Indicators by location, that...
# tracks down reached beneficiaries per each of the targeted...
# locations and indicators per residency, that also tracks reached...
# beneficiaries by their residency statuses (IDPs, Returnees, Residents).
indicators_list = PMF['Indicators'].tolist()
inds = []       #Raw indicators
res = []        #This will hold indicators disaggregated by residency statuses
loc = []        # This will hold indicators disaggregated by location.
for i in indicators_list:
    inds.append({'Indicators':i, str(dd):None, str(dd)+' (AGFY)':None})
    res.append({'Indicators':i, 'Residents':0.0,'IDPs':0.0, 'Returnees':0.0, 
                 'Residents (AGFY)':0.0, 'IDPs (AGFY)': 0.0, 'Returnees (AGFY)':0.0,'Last Update':None})
    loc.append({'Indicators':i, 'Damascus':0.0, 'Aleppo':0.0, 'Salamiyeh':0.0, 
                 'Damascus (AGFY)':0.0, 'Aleppo (AGFY)':0.0, 'Salamiyeh (AGFY)':0.0,'Last Update':None})

indicators = pd.DataFrame.from_records(inds)
indicators_by_residency = pd.DataFrame.from_records(res)
indicators_by_location = pd.DataFrame.from_records(loc)
indicators = indicators.set_index('Indicators')
indicators_by_location = indicators_by_location.set_index('Indicators')
indicators_by_residency = indicators_by_residency.set_index('Indicators')

# Calculating indicators and saving them to indicators' structures above.
data = pd.read_sql('SELECT * FROM OUTPUT_REGISTRATION', con)
data['_submission_time'] = pd.to_datetime(data['_submission_time'])
data = data[data['_submission_time'].dt.month == last_month]

inds_acts = [
    ("1111.1 # of adolescent girls and female youth from girls' clubs/peer-to-peer groups enrolled in life skills sessions (by type, age, location).", 'act_1111.4'),
    ("1112.1 # of adolescent girls and female youth mentored (by age, country)", 'act_1112.3'),
    ("1122.1 # of community members reached by outreach sessions/training on SGBV/SRHR (by sex, age, location and type of session).", 'act_1122.1'),
    ("1123.1 # of community stakeholders trained on gender-responsive design and implementation of community initiatives that respond to the needs of adolescent girls and female youth (by sex, age, location).", 'act_1123.1'),
    ("1211.1 # of formal/non-formal teachers and school leaders trained on basic, gender-responsive and inclusive pedagogical approaches (by type, sex, location).", 'act_1211.3'),
    ("1212.1 # of formal/non-formal teachers and school leaders trained on the provision of safe and secure and gender-sensitive learning environments (by type, sex, location).", 'act_1212.2'),
    ("1223.1 # of adolescents girls and female youth trained on vocational skills (by sex, age, location).", 'act_1223.6'),
    ("1224.1 # learners who are out of school or at risk of dropping out of formal education receiving support (by sex, age, location).", 'act_1224.3')
]

for index, (ind, act) in enumerate(inds_acts):
    indicators.at[ind, str(dd)] = data[data['activity'] == act].count()['index']
    indicators.at[ind, str(dd)+' (AGFY)'] = data[(data['activity'] == act) & (data['ben_sex'] == 'Female')].count()['index']

    indicators_by_location.at[ind, 'Damascus'] = data[(data['activity'] == act) & ((data['location'] == 'Damascus_Tadamon') | (data['location'] == 'Damascus_Old_City'))].count()['index']
    indicators_by_location.at[ind, 'Aleppo'] = data[(data['activity'] == act) & ((data['location'] == 'Aleppo_Hanano') | (data['location'] == 'Aleppo_Old_City'))].count()['index']
    indicators_by_location.at[ind, 'Salamiyeh'] = data[(data['activity'] == act) & ((data['location'] == 'Salamiyeh_City') | (data['location'] == 'Salamiyeh_Rural'))].count()['index']

    indicators_by_location.at[ind, 'Damascus (AGFY)'] = data[(data['activity'] == act) & (data['ben_sex'] == 'Female') & ((data['location'] == 'Damascus_Tadamon') | (data['location'] == 'Damascus_Old_City'))].count()['index']
    indicators_by_location.at[ind, 'Aleppo (AGFY)'] = data[(data['activity'] == act) & (data['ben_sex'] == 'Female') & ((data['location'] == 'Aleppo_Hanano') | (data['location'] == 'Aleppo_Old_City'))].count()['index']
    indicators_by_location.at[ind, 'Salamiyeh (AGFY)'] = data[(data['activity'] == act) & (data['ben_sex'] == 'Female') & ((data['location'] == 'Salamiyeh_City') | (data['location'] == 'Salamiyeh_Rural'))].count()['index']
    indicators_by_location.at[ind, 'Last Update'] = str(dd)

    indicators_by_residency.at[ind, 'Residents'] = data[(data['activity'] == act) & (data['ben_residency_status'] == 'Resident')].count()['index']
    indicators_by_residency.at[ind, 'IDPs'] = data[(data['activity'] == act) & (data['ben_residency_status'] == 'IDP')].count()['index']
    indicators_by_residency.at[ind, 'Returnees'] = data[(data['activity'] == act) & (data['ben_residency_status'] == 'Returnee')].count()['index']

    indicators_by_residency.at[ind, 'Residents (AGFY)'] = data[(data['activity'] == act)  & (data['ben_sex'] == 'Female') & (data['ben_residency_status'] == 'Resident')].count()['index']
    indicators_by_residency.at[ind, 'IDPs (AGFY)'] = data[(data['activity'] == act) & (data['ben_sex'] == 'Female') & (data['ben_residency_status'] == 'IDP')].count()['index']
    indicators_by_residency.at[ind, 'Returnees (AGFY)'] = data[(data['activity'] == act) & (data['ben_sex'] == 'Female') & (data['ben_residency_status'] == 'Returnee')].count()['index']
    indicators_by_residency.at[ind, 'Last Update'] = str(dd)

# Calculation of Indicators 1131.1 & 1132.1
# Data related to these two indicators is stored outside the database...
# and outside Kobo, and have different calculation logic.
sheet_id = credentials['GRANTS_MONITORING_SHEET_ID']
data_1131_1 = pd.read_excel(f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx', sheet_name='DATASET')
data_1131_1_ = data_1131_1[data_1131_1['SUBMISSION STATUS'] == 'Approved']
data_1131_1.to_sql('GRANTS_TRACKING_MATRIX', con = con, if_exists='replace')

ind_1131_1 = data_1131_1_.iloc[:, 13:19].sum(axis=1).sum()
ind_1132_1 = data_1131_1_.count()['SERIAL']

inds_1130 = [
    "1131.1 # of adolescent girls and female youth reached by financial and/or in-kind assistance (by type of support, age, location).",
    "1132.1 # of community-response fund grants disbursed (location)."
]

indicators.at[inds_1130[0], str(dd)] = ind_1131_1
indicators.at[inds_1130[0], str(dd)+ ' (AGFY)'] = ind_1131_1
indicators.at[inds_1130[1], str(dd)] = ind_1132_1
indicators.at[inds_1130[1], str(dd)+ ' (AGFY)'] = 0.0

locations = ["Damascus","Aleppo","Salamiyeh"]

for l in locations:
    indicators_by_location.at[inds_1130[0], l] = data_1131_1_[(data_1131_1_["LOCATION"] == l)].iloc[:, 13:19].sum(axis=1).sum()
    indicators_by_location.at[inds_1130[0], l + ' (AGFY)'] = data_1131_1_[(data_1131_1_["LOCATION"] == l)].iloc[:, 13:19].sum(axis=1).sum()
    indicators_by_location.at[inds_1130[1], l] = data_1131_1_[data_1131_1_["LOCATION"] == l].count()['SERIAL']
    indicators_by_location.at[inds_1130[1], l + ' (AGFY)'] = 0.0
indicators_by_location.at[inds_1130[0],'Last Update'] = str(dd)
indicators_by_location.at[inds_1130[1],'Last Update'] = str(dd)

statuses = [("Residents", "FEMALE BENEFICIARIES (10 - 15) - RESIDENTS", "FEMALE BENEFICIARIES (16 - 18) - RESIDENTS"),
            ("IDPs", "FEMALE BENEFICIARIES (10 - 15) - IDPs", "FEMALE BENEFICIARIES (16 - 18) - IDPs"),
            ("Returnees", "FEMALE BENEFICIARIES (10 - 15) - RETURNEES", "FEMALE BENEFICIARIES (16 - 18) - RETURNEES")]

for index, (s1, s2, s3) in enumerate(statuses):
    indicators_by_residency.at[inds_1130[0], s1] = data_1131_1_[s2].sum() + data_1131_1_[s3].sum()
    indicators_by_residency.at[inds_1130[0], s1 + ' (AGFY)'] = data_1131_1_[s2].sum() + data_1131_1_[s3].sum()
    indicators_by_residency.at[inds_1130[1], s1] = 0.0
    indicators_by_residency.at[inds_1130[1], s1 + ' (AGFY)'] = 0.0
indicators_by_residency.at[inds_1130[0], 'Last Update'] = str(dd)
indicators_by_residency.at[inds_1130[1], 'Last Update'] = str(dd)

#Updating PMF:
PMF['#Progress'] = PMF['#Progress'].apply(int) + indicators[str(dd)].fillna(0).values
PMF['#Progress(AGFY)'] = PMF['#Progress(AGFY)'].apply(int) + indicators[str(dd)+ ' (AGFY)'].fillna(0).values 

for i in range(len(PMF)):
    if PMF['Target'].iloc[i] == 0:
        PMF['%Progress'].iloc[i] = 0.0
    else:
        PMF['%Progress'].iloc[i] = PMF['#Progress'].iloc[i] / PMF['Target'].iloc[i] 
        
for i in range(len(PMF)):
    if PMF['Target(AGFY)'].iloc[i] == 0:
        PMF['%Progress(AGFY)'].iloc[i] = 0.0
    else:
        PMF['%Progress(AGFY)'].iloc[i] = PMF['#Progress(AGFY)'].iloc[i] / PMF['Target(AGFY)'].iloc[i] 
PMF[str(dd)]  = indicators[str(dd)].fillna(0).values
PMF[str(dd) + ' (AGFY)'] = indicators[str(dd)+ ' (AGFY)'].fillna(0).values
PMF.to_sql('AGENCI_OUTPUTS_PMF', con = con, if_exists='replace')

#Updating INDICATORS_BY_LOCATION...
INDICATORS_BY_LOCATION_ = pd.read_sql('SELECT * FROM INDICATORS_BY_LOCATION', con)
INDICATORS_BY_LOCATION_ = INDICATORS_BY_LOCATION_.set_index('Indicators')
INDICATORS_BY_LOCATION_ = INDICATORS_BY_LOCATION_.drop(columns = ['index'])
INDICATORS_BY_LOCATION_ = INDICATORS_BY_LOCATION_.fillna(0.0)
INDICATORS_BY_LOCATION_.iloc[:, 0:6] = INDICATORS_BY_LOCATION_.iloc[:, 0:6] + indicators_by_location.iloc[:, 0:6]
INDICATORS_BY_LOCATION_['Last Update'] = str(dd)
INDICATORS_BY_LOCATION_.to_sql('INDICATORS_BY_LOCATION', con = con, if_exists = 'replace')

#Updating INDICATORS_BY_RESIDENCY...
INDICATORS_BY_RESIDENCY_ = pd.read_sql('SELECT * FROM INDICATORS_BY_RESIDENCY', con)
INDICATORS_BY_RESIDENCY_ = INDICATORS_BY_RESIDENCY_.set_index('Indicators')
INDICATORS_BY_RESIDENCY_ = INDICATORS_BY_RESIDENCY_.drop(columns = ['index'])
INDICATORS_BY_RESIDENCY_ = INDICATORS_BY_RESIDENCY_.fillna(0.0)
INDICATORS_BY_RESIDENCY_.iloc[:, 0:6] = INDICATORS_BY_RESIDENCY_.iloc[:, 0:6] + indicators_by_residency.iloc[:, 0:6]
INDICATORS_BY_RESIDENCY_['Last Update'] = str(dd)
INDICATORS_BY_RESIDENCY_.to_sql('INDICATORS_BY_RESIDENCY', con = con, if_exists = 'replace')

con.close()
