
# The following algorithm is designed to conduct data quality check for all data collected
# using AGENCI outputs monitoring data collection tool quicker and with more accuracy. 
# This file shows the Python code of this algorithm along with explanation of what was done to achieve the desired product.
# This algorithm, as mentioned above, will extract data via Kobo API, conduct data quality check, 
# put all data that is not up to the deired quality in one file which will be sent to data collectors for 
# clarification and/or necessary correction, and store the cleaned up data in a designated database. 
# It will also make it easier to implement quality check should quality criteria is modified for any reason.

import numpy as np
import pandas as pd
import datetime as dt
import urllib.request
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
#OUTPUT_MONITORING_TOOL = os.environ.get('AGENCI_OUTPUT_MONITORING_TOOL')

# Establishing database connection...
con = sqlite3.connect(credentials['AGENCI_DATABASE_CONNECTION'])
cur = con.cursor()

# Fetching data from Kobo using the live Kobo API link that is...
# saved on the protected credentials JSON file.

with urllib.request.urlopen(credentials['OUTPUT_MONITORING_TOOL']) as url:
    data = json.loads(url.read())

df = pd.json_normalize(data, 'results')

#Renaming dataframe columns for the ease of use...
df = df.rename(
    columns = {
        'day':'submission_date',
        'nam_num':'activity',
        'sub_nam_num':'indicator',
        'ben_per_info/ben_dob':'ben_dob',
        'ac_resp':'activity_responsible',
        'area':'location',
        'ben_per_info/ben_name':'ben_name',
        'ben_per_info/mother_name':'mother_name',
        'ben_per_info/ben_age':'ben_age',
        'ben_per_info/ben_sex':'ben_sex',
        'ben_per_info/ID_0':'ben_ID',
        'ben_per_info/ben_job':'ben_job',
        'ben_per_info/mobile_phone':'ben_has_mobile',
        'ben_per_info/mobile_num':'ben_mobile_num',
        'ben_per_info/landline_phone':'ben_has_landline',
        'ben_per_info/landline_num':'ben_landline_num',
        'ben_per_info/idp_host':'ben_residency_status',
        'ben_per_info/ben_add':'ben_address',
        'ben_per_info/dis':'ben_has_disability',
        'ben_per_info/dis_kind':'ben_disability_type',
        'ben_per_info/in_out_school':'ben_in_out_school',
        'ben_per_info/out_class':'ben_out_class', 
        'ben_per_info/in_class':'ben_in_class'
    }
)

# Dropping columns that are not necessary.
df = df.drop(columns=['ben_per_info/ben_dob_number', '__version__', 'meta/instanceID', 'meta/instanceName',
       '_xform_id_string', '_uuid', '_attachments', '_status', '_geolocation','_tags', '_notes', 'ben_per_info/ben_current_date'])

# This algorithm runs on a monthly basis, and performs quality...
# check on data collected last month. Therefore, the data imported...
# from Kobo API is filtered by date to only include, QC and save...
# unsaved data that was inserted last month.
df['_submission_time'] = pd.to_datetime(df['_submission_time'])
this_month = pd.to_datetime('today').month
last_month = this_month - 1
df = df[df['_submission_time'].dt.month == last_month]
df['ben_full_name'] = df['ben_name'] + ' - ' + df['mother_name']

# Empty dataframe 'good_data' was created as a placeholder...
# to save quality checked data that meets the quality criteria.
good_data = pd.DataFrame()


"""
While examining the data quality check criteria for the activities included
in the AGENCI output monitoring tool, it appeared that some activities shared similar criteria.
As such and in order to make the algorithm shorter and more efficient, 
activities with similar data quality check criteria were put together in categories.

The first category of activities included the following activities:

* 1111.4 Deliver life-skills training to girls and young women through newly established or existing community learning settings.
* 1111.5 Host exchange meeting between girls' clubs to provide girls with the opportunity to network beyond their immediate peer groups.
* 1112.3 Connect targeted girls with Canada-based mentors online for regular discussions.
* 1112.6 Facilitate confidence-building and mentorship session between mentors and girls participating in girls' clubs.
* 1112.7 Facilitate leadership workshops with identified adolescent girls and female youth participants.

These activities shared the following data quality criteria:

* Gender of beneficiary = 'Female'.
* Age of beneficiaries is between 10 and 24 years of age (including 10 and 24).
* ID numbers for beneficiaries who are 14 years of age and older is not Null.
* Whether the beneficiary is at school or outside school is not Null.
"""
acts1=[
    "act_1111.4",
    "act_1111.5",
    "act_1112.3",
    "act_1112.6",
    "act_1112.7"
]

for a in acts1:
    good = df[df["activity"] == a]
    good = good[
        (good['ben_sex'] == 'Female') &
        ((good['ben_age'].apply(int) >= 10) & good['ben_age'].apply(int) <= 24) &
        ((good['ben_age'].apply(int) >=14) & good['ben_ID'].isna() == False) &
        (good['ben_in_out_school'].isna() == False)
    ]
    
    good = good.drop_duplicates(subset='ben_full_name')
    good_data = pd.concat([good_data, good])

"""
The second category of activities include:​

* 1122.1 Engage elders and chiefs to promote girls' enrolment in schools.
* 1123.1 Identify and train community leaders and other stakeholders with social, cultural, 
and political capital to address socio-cultural norms that serve to marginalize and exclude 
girls and women from education and other opportunities.
* 1211.3 Deliver training to non-formal teachers to provide 
self-learning curriculum to out of school adolescent girls and girls at risk of dropping out.
* 1212.2 Train non-formal community management groups on gender responsive education, 
prevention of SGBV, safeguarding, creating safe and accessible learning environments, 
student intake management and conducting needs assessments.​
and they share the following criteria:​
* Gender of beneficiary = 'Female'.
* Beneficiaries are older than 18 years of age.
* ID numbers of beneficiaries are not NULL.
* Whetehr a beneficiary is in or out of school is not needed.
"""

acts2 = [
    "act_1122.1",
    "act_1123.1",
    "act_1211.3",
    "act_1212.2",
]

for a in acts2:
    good = df[df["activity"] == a]
    good = good[
        (good['ben_sex'] == 'Female') &
        (good['ben_age'].apply(int) > 18) &
        (good['ben_ID'].isna() == False) &
        (good['ben_in_out_school'].isna())
    ]
    
    good = good.drop_duplicates(subset='ben_full_name')
    good_data = pd.concat([good_data, good])


"""
The third category of activities includes:

*1111.2 Train existing and new facilitators to form girls' clubs/peer-to-peer 
groups and deliver life skills curricula using gender-responsive facilitation approaches.
* 1223.4 Conduct follow-up with girls pursuing apprenticeship pathways.
* 1224.1 Organize gender inclusive community-based non-formal education management 
groups with community leaders, education actors and parents.

And they share the following criteria:

* Gender of beneficiaries = 'Female'.
* Beneficiaries are 18 years of age and older.
* ID numbers of beneficiaries are not NULL.
* Whether or not a beneficiary is enrolled at school is not needed.
"""

acts3 = [
    "act_1111.2",
    "act_1223.4",
    "act_1224.1"
]

for a in acts3:
    good = df[df["activity"] == a]
    good = good[
        (good['ben_sex'] == 'Female') &
        (good['ben_age'].apply(int) >= 18) &
        (good['ben_ID'].isna() == False) &
        (good['ben_in_class'].isna()) &
        (good['ben_out_class'].isna())
    ]
    
    good = good.drop_duplicates(subset='ben_full_name')
    good_data = pd.concat([good_data, good])


"""
The fourth category of activities includes:

* 1223.2 Train local business owners on apprenticeship programs and expectations, including safety of girls.

This activity has this data quality criteria:

* Gender of beneficiaries can be 'Male' or 'Female'.
* Beneficiaries are older than 18 years of age.
* ID numbers of beneficiaries are recorded.
* Whether or not beneficiaries are enrolled at school is irrelevant.
"""

acts4 = [
    "act_1223.2"
]

for a in acts4:    
    good = df[df["activity"] == a]
    good = good[
        (good['ben_age'].apply(int) > 18) &
        (good['ben_ID'].isna() == False) &
        (good['ben_in_class'].isna()) &
        (good['ben_out_class'].isna())
    ]
    
    good = good.drop_duplicates(subset='ben_full_name')
    good_data = pd.concat([good_data, good])

"""
The fifth category includes only this activity:

* 1223.6 Deliver entrepreneurship and skills based vocational courses through community learning centres and schools.

and it has the following data quality criteria:

* Beneficiaries can be males or females.
* Beneficiaries should be 18 years of age or older.
* ID numbers of beneficiaries should be registered.
* Whether or not beneficiaries are enrolled at school is irrelevant.
"""

acts5 = [
    "act_1223.6"
]

for a in acts5:
    good = df[df["activity"] == a]
    good = good[
        (good['ben_age'].apply(int) >= 18) &
        (good['ben_ID'].isna() == False) &
        (good['ben_in_class'].isna()) &
        (good['ben_out_class'].isna())
    ]
    
    good = good.drop_duplicates(subset='ben_full_name')
    good_data = pd.concat([good_data, good])

"""
The sixth and final category of activities includes the following one:

* 1224.3 Provide self-learning education support (including science, math and languages) 
to out of school adolescent girls and girls at risk of dropping out.

and it has the following criteria:

* Beneficiaries should be females only.
* Beneficiaries should be between 10 and 24 years of age (including 10 and 24).
* ID numbers for beneficiaries who are 14 years old or older should be included.
* This activity only includes females who are out of school.
"""

acts6 = [
    "act_1224.3"
]

for a in acts6:    
    good = df[df["activity"] == a]
    good = good[
        (good['ben_sex'] == 'Female') &
        ((good['ben_age'].apply(int) >= 10) & good['ben_age'].apply(int) <= 24) &
        ((good['ben_age'].apply(int) >=14) & good['ben_ID'].isna() == False) &
        (good['ben_in_out_school'] == 'Out_of_School')
    ]
    
    good = good.drop_duplicates(subset='ben_full_name')
    good_data = pd.concat([good_data, good])


# All remaining data that do not meet the QC criteria...
# will be put in a different dataframe as 'bad_data'
bad_data = pd.concat([df, good_data])
bad_data = bad_data.drop_duplicates(keep=False)

# Bad data is stored in an Excel file to be shared with data entry team...
# for necessary explanation or action.
filename = credentials['AGENCI_OUTPUTS_REJECTED_DATA'] + str(dt.date.today()) + '.xlsx'
bad_data.to_excel(filename)

# Finally, push 'good_data' to the AGENCI database.
# Make sure to change 'replace' to 'append' when real data is populated...
good_data.to_sql('OUTPUT_REGISTRATION', con = con, if_exists='replace')

con.close()