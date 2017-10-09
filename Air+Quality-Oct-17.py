
# coding: utf-8

# In[2]:


import ibmos2spark

# @hidden_cell
credentials = {
    'auth_url': 'https://identity.open.softlayer.com',
    'project_id': 'b253e4f72e7446699c05f6a72f4d9b2a',
    'region': 'dallas',
    'user_id': 'a96927c5d46b4635b65a5343ff797167',
    'username': 'member_e702b6a15bf4fe0dd0bc46c315116c7f0296b6ef',
    'password': 'eHXWY0Y*)bO8Off&'
}

configuration_name = 'os_5af4beb9b9084580b5b3df962e474145_configs'
bmos = ibmos2spark.bluemix(sc, credentials, configuration_name)

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
air_quality = spark.read  .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')  .option('header', 'true')  .load(bmos.url('AirQuality', 'annual_conc_by_monitor_2017.csv'))
air_quality.take(5)


# In[2]:

air_quality.printSchema()


# In[3]:

import pandas as pd
import numpy as np
import seaborn as sns

import matplotlib.pyplot as plt
# matplotlib.patches allows us create colored patches, we can use for legends in plots
import matplotlib.patches as mpatches
# seaborn also builds on matplotlib and adds graphical features and new plot types
#adjust settings
get_ipython().magic(u'matplotlib inline')
sns.set_style("white")
plt.rcParams['figure.figsize'] = (12, 12)


# In[11]:

airQuality_df = air_quality
airQuality_pd = airQuality_df[airQuality_df['LATITUDE'] != 0][['Parameter Code','Latitude', 'Longitude', 'Datum', 'Parameter Name',
                                                               'Sample Duration', 'Pollutant Standard', 'Metric Used',
                                                               'Year', 'Observation Count', 'Completeness Indicator',
                                                               'Local Site Name', 'State Name', 'County Name', 'Date of Last Change']].toPandas()

airQuality_pd.columns = ['Parameter Code','Latitude', 'Longitude', 'Datum', 'Parameter Name',
                                                               'Sample Duration', 'Pollutant Standard', 'Metric Used',
                                                               'Year', 'Observation Count', 'Completeness Indicator',
                                                               'Local Site Name', 'State Name', 'County Name', 'Date of Last Change']

airQuality_pd['Parameter Code'] = airQuality_pd['Parameter Code'].astype(int) 
airQuality_pd['Latitude'] = airQuality_pd['Latitude'].astype(float)
airQuality_pd['Longitude'] = airQuality_pd['Longitude'].astype(float)
airQuality_pd['Year'] = airQuality_pd['Year'].astype(int)
airQuality_pd['Observation Count'] = airQuality_pd['Observation Count'].astype(int)
airQuality_pd['Completeness Indicator'] = airQuality_pd['Completeness Indicator'].astype(bool)

airQuality_pd['Parameter Code'].unique()


# In[4]:

airQuality_df = air_quality
airQuality_pd = airQuality_df[airQuality_df['LATITUDE'] != 0][['Parameter Code','Latitude', 'Longitude', 'Datum', 'Parameter Name',
                                                               'Sample Duration', 'Pollutant Standard', 'Metric Used',
                                                               'Year', 'Observation Count', 'Completeness Indicator',
                                                               'Local Site Name', 'State Name', 'County Name', 'Date of Last Change']].toPandas()

airQuality_pd.columns = ['Parameter Code','Latitude', 'Longitude', 'Datum', 'Parameter Name',
                                                               'Sample Duration', 'Pollutant Standard', 'Metric Used',
                                                               'Year', 'Observation Count', 'Completeness Indicator',
                                                               'Local Site Name', 'State Name', 'County Name', 'Date of Last Change']

airQuality_pd['Parameter Code'] = airQuality_pd['Parameter Code'].astype(float) 
airQuality_pd['Latitude'] = airQuality_pd['Latitude'].astype(float)
airQuality_pd['Longitude'] = airQuality_pd['Longitude'].astype(float)
airQuality_pd['Year'] = airQuality_pd['Year'].astype(float)
airQuality_pd['Observation Count'] = airQuality_pd['Observation Count'].astype(float)
airQuality_pd['Completeness Indicator'] = airQuality_pd['Completeness Indicator'].astype(bool)

ozone_pd = airQuality_pd[airQuality_pd['Parameter Code']==44201]
pm25_local_conditions_pd = airQuality_pd[airQuality_pd['Parameter Code']==88101]
acceptable_pm25_AQI_speciation_mass_pd = airQuality_pd[airQuality_pd['Parameter Code']==88502]
CO3_pd = airQuality_pd[airQuality_pd['Parameter Code']==42101]


# In[13]:

plt.scatter(airQuality_pd.Longitude, airQuality_pd.Latitude, alpha=0.05, s=4, color='darkseagreen')

#adjust more settings
plt.title('Air Quality', size=25)
plt.xlabel('Longitude',size=20)
plt.ylabel('Latitude',size=20)

plt.show()


# In[10]:

alabama = airQuality_pd[airQuality_pd['State Name']=='Alabama']
alaska = airQuality_pd[airQuality_pd['State Name']=='Alaska']
arizona = airQuality_pd[airQuality_pd['State Name']=='Arizona']
arkansas = airQuality_pd[airQuality_pd['State Name']=='Arkansas'] 
california = airQuality_pd[airQuality_pd['State Name']=='California'] 
california = airQuality_pd[airQuality_pd['State Name']=='California'] 
colorado = airQuality_pd[airQuality_pd['State Name']=='Colorado'] 
colorado = airQuality_pd[airQuality_pd['State Name']=='Colorado'] 
connecticut = airQuality_pd[airQuality_pd['State Name']=='Connecticut'] 
delaware = airQuality_pd[airQuality_pd['State Name']=='Delaware'] 
dc = airQuality_pd[airQuality_pd['State Name']=='District Of Columbia'] 
florida = airQuality_pd[airQuality_pd['State Name']=='Florida'] 
georgia = airQuality_pd[airQuality_pd['State Name']=='Georgia'] 
hawaii = airQuality_pd[airQuality_pd['State Name']=='Hawaii'] 
idaho = airQuality_pd[airQuality_pd['State Name']=='Idaho'] 
illinois = airQuality_pd[airQuality_pd['State Name']=='Illinois'] 
indiana = airQuality_pd[airQuality_pd['State Name']=='Indiana'] 
iowa = airQuality_pd[airQuality_pd['State Name']=='Iowa'] 
kansas = airQuality_pd[airQuality_pd['State Name']=='Kansas'] 
kentucky = airQuality_pd[airQuality_pd['State Name']=='Kentucky'] 
louisiana = airQuality_pd[airQuality_pd['State Name']=='Louisiana'] 
maine = airQuality_pd[airQuality_pd['State Name']=='Maine'] 
maryland = airQuality_pd[airQuality_pd['State Name']=='Maryland'] 
massachusetts = airQuality_pd[airQuality_pd['State Name']=='Massachusetts'] 
michigan = airQuality_pd[airQuality_pd['State Name']=='Michigan'] 
minnesota = airQuality_pd[airQuality_pd['State Name']=='Minnesota'] 
missouri = airQuality_pd[airQuality_pd['State Name']=='Missouri'] 
montana = airQuality_pd[airQuality_pd['State Name']=='Montana'] 
nebraska = airQuality_pd[airQuality_pd['State Name']=='Nebraska'] 
nevada = airQuality_pd[airQuality_pd['State Name']=='Nevada'] 
newMexico = airQuality_pd[airQuality_pd['State Name']=='New Mexico'] 
newYork = airQuality_pd[airQuality_pd['State Name']=='New York'] 
northCarolina = airQuality_pd[airQuality_pd['State Name']=='North Carolina'] 
northDakota = airQuality_pd[airQuality_pd['State Name']=='North Dakota'] 
ohio = airQuality_pd[airQuality_pd['State Name']=='Ohio'] 
oklahoma = airQuality_pd[airQuality_pd['State Name']=='Oklahoma'] 
pennsylvania = airQuality_pd[airQuality_pd['State Name']=='Pennsylvania'] 
rhodeIsland = airQuality_pd[airQuality_pd['State Name']=='Rhode Island']
southCarolina = airQuality_pd[airQuality_pd['State Name']=='South Carolina'] 
southDakota = airQuality_pd[airQuality_pd['State Name']=='South Dakota'] 
tennessee = airQuality_pd[airQuality_pd['State Name']=='Tennessee'] 
texas = airQuality_pd[airQuality_pd['State Name']=='Texas'] 
utah = airQuality_pd[airQuality_pd['State Name']=='Utah'] 
vermont = airQuality_pd[airQuality_pd['State Name']=='Vermont'] 
virginia = airQuality_pd[airQuality_pd['State Name']=='Virginia'] 
washington = airQuality_pd[airQuality_pd['State Name']=='Washington'] 
westVirginia = airQuality_pd[airQuality_pd['State Name']=='West Virginia'] 
wisconsin = airQuality_pd[airQuality_pd['State Name']=='Wisconsin'] 
wyoming = airQuality_pd[airQuality_pd['State Name']=='Wyoming'] 
countryOfMexico = airQuality_pd[airQuality_pd['State Name']=='Country Of Mexico']  



plt.scatter(alabama.Longitude, alabama.Latitude,s=1, color='blue', marker ='.')
plt.scatter(alaska.Longitude, alaska.Latitude,s=1, color='orange', marker ='.')
plt.scatter(arizona.Longitude, arizona.Latitude,s=1, color='pink', marker ='.')
plt.scatter(arkansas.Longitude, arkansas.Latitude,s=1, color='red', marker ='.')
plt.scatter(california.Longitude, california.Latitude, s=1, color='green', marker ='.')

plt.scatter(colorado.Longitude, colorado.Latitude, s=1, color='yellow', marker ='.')
plt.scatter(connecticut.Longitude, connecticut.Latitude, color='purple', s=1, marker ='.')
plt.scatter(delaware.Longitude, delaware.Latitude, s=1, color='olive', marker ='.')
plt.scatter(dc.Longitude, dc.Latitude, s=1, color='maroon', marker ='.')

plt.scatter(florida.Longitude, florida.Latitude, color='aqua', s=1, marker ='.')
plt.scatter(georgia.Longitude, georgia.Latitude, s=1, color='teal', marker ='.')
plt.scatter(hawaii.Longitude, hawaii.Latitude, s=1, color='purple', marker ='.')

plt.scatter(idaho.Longitude, idaho.Latitude, color='navy', s=1, marker ='.')
plt.scatter(illinois.Longitude, illinois.Latitude, s=1, color='green', marker ='.')
plt.scatter(indiana.Longitude, indiana.Latitude, s=1, color='black', marker ='.')


plt.scatter(iowa.Longitude, iowa.Latitude, color='red', s=1, marker ='.')
plt.scatter(kansas.Longitude, kansas.Latitude, s=1, color='blue', marker ='.')
plt.scatter(kentucky.Longitude, kentucky.Latitude, s=1, color='red', marker ='.')


plt.scatter(louisiana.Longitude, louisiana.Latitude, color='olive', s=1, marker ='.')
plt.scatter(maine.Longitude, maine.Latitude, s=1, color='maroon', marker ='.')
plt.scatter(maryland.Longitude, maryland.Latitude, s=1, color='pink', marker ='.')


plt.scatter(massachusetts.Longitude, massachusetts.Latitude, color='orange', s=1, marker ='.')
plt.scatter(michigan.Longitude, michigan.Latitude, s=1, color='navy', marker ='.')
plt.scatter(missouri.Longitude, missouri.Latitude, s=1, color='red', marker ='.')

plt.scatter(iowa.Longitude, iowa.Latitude, color='red', s=1, marker ='.')
plt.scatter(kansas.Longitude, kansas.Latitude, s=1, color='olive', marker ='.')
plt.scatter(kentucky.Longitude, kentucky.Latitude, s=1, color='teal', marker ='.')


plt.scatter(minnesota.Longitude, minnesota.Latitude, color='silver', s=1, marker ='.')
plt.scatter(montana.Longitude, montana.Latitude, s=1, color='orange', marker ='.')
plt.scatter(nebraska.Longitude, nebraska.Latitude, s=1, color='red', marker ='.')


plt.scatter(nevada.Longitude, nevada.Latitude, color='black', s=1, marker ='.')
plt.scatter(newMexico.Longitude, newMexico.Latitude, s=1, color='pink', marker ='.')
plt.scatter(newYork.Longitude, newYork.Latitude, s=1, color='yellow', marker ='.')

plt.scatter(northCarolina.Longitude, northCarolina.Latitude, color='navy', s=1, marker ='.')
plt.scatter(northDakota.Longitude, northDakota.Latitude, s=1, color='green', marker ='.')
plt.scatter(ohio.Longitude, ohio.Latitude, s=1, color='black', marker ='.')

plt.scatter(oklahoma.Longitude, oklahoma.Latitude, color='red', s=1, marker ='.')
plt.scatter(pennsylvania.Longitude, pennsylvania.Latitude, s=1, color='green', marker ='.')
plt.scatter(rhodeIsland.Longitude, rhodeIsland.Latitude, s=1, color='black', marker ='.')

plt.scatter(southCarolina.Longitude, southCarolina.Latitude, color='red', s=1, marker ='.')
plt.scatter(southDakota.Longitude, southDakota.Latitude, s=1, color='green', marker ='.')
plt.scatter(tennessee.Longitude, tennessee.Latitude, s=1, color='black', marker ='.')

plt.scatter(texas.Longitude, texas.Latitude, color='red', s=1, marker ='.')
plt.scatter(utah.Longitude, utah.Latitude, s=1, color='green', marker ='.')
plt.scatter(vermont.Longitude, vermont.Latitude, s=1, color='black', marker ='.')

plt.scatter(virginia.Longitude, virginia.Latitude, color='red', s=1, marker ='.')
plt.scatter(westVirginia.Longitude, westVirginia.Latitude, s=1, color='green', marker ='.')
plt.scatter(washington.Longitude, washington.Latitude, s=1, color='black', marker ='.')

plt.scatter(wisconsin.Longitude, wisconsin.Latitude, color='red', s=1, marker ='.')
plt.scatter(wyoming.Longitude, wyoming.Latitude, s=1, color='green', marker ='.')
plt.scatter(countryOfMexico.Longitude, countryOfMexico.Latitude, s=1, color='black', marker ='.')


plt.title('Air Quality by State', size=20)
plt.xlabel('Longitude',size=20)
plt.ylabel('Latitude',size=20)
plt.show()


airQuality_pd['Observation Count'] = airQuality_pd['Observation Count'].astype(int)

alabama_25_Local_Conditions_filter1 = airQuality_pd[np.logical_and(airQuality_pd['State Name']=='Alabama', airQuality_pd['Parameter Name']=='PM2.5 - Local Conditions')]
alabama_25_Local_Conditions_filter2 = alabama_25_Local_Conditions_filter1[np.logical_and(alabama_25_Local_Conditions_filter1['Metric Used']=='Daily Mean', alabama_25_Local_Conditions_filter1['Sample Duration']=='24 HOUR')]

alabama_good = alabama_25_Local_Conditions_filter2[np.logical_and(['Observation Count'] < 51),True]

plt.scatter(alabama_good.Longitude, alabama_good.Latitude, color='red', s=5, marker ='.')



# In[ ]:




# In[ ]:




# In[39]:

airQuality_pd['Observation Count'] = airQuality_pd['Observation Count'].astype(int)

alabama_25_Local_Conditions_filter1 = airQuality_pd[np.logical_and(airQuality_pd['Observation Count'] < 302, airQuality_pd['Parameter Name']=='PM2.5 - Local Conditions')]
alabama_25_Local_Conditions_filter2 = alabama_25_Local_Conditions_filter1[np.logical_and(alabama_25_Local_Conditions_filter1['Metric Used']=='Daily Mean', alabama_25_Local_Conditions_filter1['Sample Duration']=='24 HOUR')]

allStates_good = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] < 51,alabama_25_Local_Conditions_filter2['Observation Count'] < 51)]
allStates_moderate = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] > 51,alabama_25_Local_Conditions_filter2['Observation Count'] < 101)]
allStates_USG = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] > 101,alabama_25_Local_Conditions_filter2['Observation Count'] < 151)]
allStates_Unhealthy = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] > 150,alabama_25_Local_Conditions_filter2['Observation Count'] < 201)]

#size
plt.figure(figsize=(15,10))

#legend
green_patch = mpatches.Patch( label='Good', color='Green')
yellow_patch = mpatches.Patch(color='yellow', label='Moderate')
navy_patch = mpatches.Patch(color='navy', label='Unhealthy for Sensitive groups')
purple_patch = mpatches.Patch(color='purple', label='Unhealthy')
plt.legend([green_patch, yellow_patch, navy_patch, purple_patch],('Good', 'Moderate', 'Unhealthy for Sensitive groups', 'Unhealthy'), 
           loc='upper left', prop={'size':20})

#plot
plt.scatter(allStates_good.Longitude, allStates_good.Latitude, color='green', s=100, marker ='*')
plt.scatter(allStates_moderate.Longitude, allStates_moderate.Latitude, color='yellow', s=100, marker  = '.')
plt.scatter(allStates_USG.Longitude, allStates_USG.Latitude, color='navy', s=100, marker  = '^')
plt.scatter(allStates_Unhealthy.Longitude, allStates_Unhealthy.Latitude, color='purple', s=100, marker  = 'v')

plt.title('Air Quality by State', size=50)
plt.xlabel('Longitude',size=50)
plt.ylabel('Latitude',size=50)
plt.show()


# In[38]:

states = airQuality_df.groupBy('State Name').count().sort('count').toPandas().iloc[1:,:]
#states = airQuality_df.groupby(['State Name']).max().toPandas().iloc[1:,:]

states['State Name'] = [u'Alabama', u'Alaska', u'Arizona', u'Arkansas', u'California', u'Colorado', u'Connecticut', u'Delaware', u'District Of Columbia', u'Florida', u'Georgia', u'Hawaii', u'Idaho', u'Illinois', u'Indiana', u'Iowa', u'Kansas', u'Kentucky', u'Louisiana', u'Maine', u'Maryland', u'Massachusetts', u'Michigan', u'Minnesota', u'Missouri', u'Montana', u'Nebraska', u'Nevada', u'New Mexico', u'New York', u'North Carolina', u'North Dakota', u'Ohio', u'Oklahoma', u'Pennsylvania', u'Rhode Island', u'South Carolina', u'South Dakota', u'Tennessee', u'Texas', u'Utah', u'Vermont', u'Virginia', u'Washington', u'West Virginia', u'Wisconsin', u'Wyoming']
#colors = ['g','0.75','y','k','b','r']
states.sort_values(by='count', ascending=True)['count'].plot.barh(color=colors)
#plt.figure(figsize=(15,10))
plt.xlabel('Samples')
plt.ylabel('States')
plt.title('Air Quality', size=15)
plt.yticks(range(0,48),states['State Name'])
plt.show()


# In[40]:

airQuality_pd['Observation Count'] = airQuality_pd['Observation Count'].astype(int)

alabama_25_Local_Conditions_filter1 = airQuality_pd[np.logical_and(airQuality_pd['Observation Count'] < 302, airQuality_pd['Parameter Name']=='PM2.5 - Local Conditions')]
alabama_25_Local_Conditions_filter2 = alabama_25_Local_Conditions_filter1[np.logical_and(alabama_25_Local_Conditions_filter1['Metric Used']=='Daily Mean', alabama_25_Local_Conditions_filter1['Sample Duration']=='24 HOUR')]

allStates_good = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] < 51,alabama_25_Local_Conditions_filter2['Observation Count'] < 51)]
allStates_moderate = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] > 51,alabama_25_Local_Conditions_filter2['Observation Count'] < 101)]
allStates_USG = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] > 101,alabama_25_Local_Conditions_filter2['Observation Count'] < 151)]
allStates_Unhealthy = alabama_25_Local_Conditions_filter2[np.logical_and(alabama_25_Local_Conditions_filter2['Observation Count'] > 150,alabama_25_Local_Conditions_filter2['Observation Count'] < 201)]

#size
plt.figure(figsize=(15,10))

#legend
green_patch = mpatches.Patch( label='Good', color='Green')
yellow_patch = mpatches.Patch(color='yellow', label='Moderate')
navy_patch = mpatches.Patch(color='navy', label='Unhealthy for Sensitive groups')
purple_patch = mpatches.Patch(color='purple', label='Unhealthy')
plt.legend([green_patch, yellow_patch, navy_patch, purple_patch],('Good', 'Moderate', 'Unhealthy for Sensitive groups', 'Unhealthy'), 
           loc='upper left', prop={'size':20})

#plot
plt.scatter(allStates_good.Longitude, allStates_good.Latitude, color='green', s=100, marker ='*')
plt.scatter(allStates_moderate.Longitude, allStates_moderate.Latitude, color='yellow', s=100, marker  = '.')
plt.scatter(allStates_USG.Longitude, allStates_USG.Latitude, color='navy', s=100, marker  = '^')
plt.scatter(allStates_Unhealthy.Longitude, allStates_Unhealthy.Latitude, color='purple', s=100, marker  = 'v')

plt.title('Air Quality by State', size=50)
plt.xlabel('Longitude',size=50)
plt.ylabel('Latitude',size=50)
plt.show()


# In[104]:

airQuality_pd['Observation Count'] = airQuality_pd['Observation Count'].astype(int)

allStates_Local_Conditions_filter1 = airQuality_pd[np.logical_and(airQuality_pd['Pollutant Standard']== 'PM25 24-hour 2012', airQuality_pd['Parameter Name']=='PM2.5 - Local Conditions')]
allStates_Local_Conditions_filter2 = allStates_Local_Conditions_filter1[np.logical_and(allStates_Local_Conditions_filter1['Metric Used']=='Daily Mean', allStates_Local_Conditions_filter1['Sample Duration']=='24 HOUR')]

allS = allStates_Local_Conditions_filter2.groupby('State Name', as_index=False)['Observation Count'].agg({'no':'max'})
print allS.values

allS.plot.barh(color='b')
plt.xlabel('PM 2.5 Count')
plt.ylabel('States')
plt.title('Air Quality by State, based on Daily Mean of observed PM2.5 Particles', size=15)
plt.yticks(range(0,35),allS['State Name'])
plt.show()


# In[ ]:



