# Use this query to extract the "User" data from Stack Exchange Data Explorer: "https://data.stackexchange.com/stackoverflow/query/new"
"""
select top 50000 * from Users
where id in (select distinct(OwnerUserID)
                          from posts where creationdate
                          between '2015-01-01'and '2015-12-31') 
   order by id;
   
select top 50000
* from Users where id in (select distinct(OwnerUserID)
                          from posts where creationdate
                          between '2015-01-01'and '2015-12-31')
             and id not in
             (select top 50000 id from users where id in (select distinct(OwnerUserID)
                          from posts where creationdate
                          between '2015-01-01'and '2015-12-31') order by id)
   order by id;

select top 50000
* from Users where id in (select distinct(OwnerUserID)
                          from posts where creationdate
                          between '2015-01-01'and '2015-03-31')
             and id not in
             (select top 100000 id from users where id in (select distinct(OwnerUserID)
                          from posts where creationdate
                          between '2015-01-01'and '2015-03-31') order by id)
   order by id;
"""





import pandas as pd
import numpy as np
import string
cd C:\Users\... <Location of data files>

# Step - 1 : Concatinate all the different chunks into a single dataset

p1 = pd.read_csv('QueryResults_1.csv')
p2 = pd.read_csv('QueryResults_2.csv')
p3 = pd.read_csv('QueryResults_3.csv')
p4 = pd.read_csv('QueryResults_4.csv')
p5 = pd.read_csv('QueryResults_5.csv')
p_all = pd.concat([p1,p2,p3,p4,p5],ignore_index = True)
p_all.to_csv('Users200k.csv')
p_all['Location'] = p_all['Location'].str.lower();
p_all.dtypes
p_all[['CreationDate', 'LastAccessDate']] = p_all[['CreationDate', 'LastAccessDate']].astype('M')
p_all['Location'] = p_all['Location'].str.strip();
##p_all['Location'] = p_all['Location'].map(lambda x: x.lstrip('+-?').rstrip('+-?'))
p_all['cleaned'] = p_all['Location'].apply(lambda x:''.join([c for c in x if c.isalpha() or (c in ', ')]))
p_all.to_csv('Users200k.csv')


###############################################################################

# Step - 2 : Imputation / Conversion to lower case / White space stripping of white spaces - On 'Locations' Variable of dataset:

p1 = pd.read_csv('Users200k.csv')
p1['Location'].fillna('No data', inplace=True) # To remove all the Nan values with a string 'No Data'
p1['cleaned_location'] = p1['Location'].apply(lambda x:''.join([c for c in x if c.isalpha() or (c in ', ')]))
p1['cleaned_location'] = p1['cleaned_location'].str.lower()
p1['cleaned_location'] = p1['cleaned_location'].str.strip(' ')
p1.to_csv('Users200k_location_cleaned.csv')


###############################################################################

# Step - 3 : Performing Geocoding to remove the location discrepancies in the dataset (spelling mistakes / non-standardized values) to derive Latitude-Longitude for the location.

import geocoder

p1 = pd.read_csv('Users200k_location_cleaned.csv')
"""output_file = "geocoder_output_Users200k.csv";
p1['cleaned_location'] = p1['Location'].apply(lambda x:''.join([c for c in x if c.isalpha() or (c in ', ')]))
p1['cleaned_location'] = p1['cleaned_location'].str.lower()
p1['cleaned_location'] = p1['cleaned_location'].str.strip(' ')"""

p1['Location_Country'] = 'NaN'
p1['Location_Lat'] = 'NaN'
p1['Location_Long'] = 'NaN'

cntry = []
loc = []
timezone1 = []

for i in range(len(p1['cleaned_location'])):
	g = geocoder.bing(p1['cleaned_location'][i], key='<SETUP A BING KEY AND REPLACE THE KEY HERE!>')
	print g.country
	p1['Location_Country'][i] = g.country
	p1['Location_Lat'][i] = g.lat
	p1['Location_Long'][i] = g.lng
	cntry.append(g.country)
	loc.append(g.latlng)

"""p1.to_csv('Users200k_withLatLong')"""
p2_temp = p1[np.isfinite(p1['Location_Lat'])]
#p2_temp = p1.loc[~p1['Location_Lat'].isin(['NaN'])]
p2_final = p2_temp.loc[p2_temp['Location_Lat'].notnull()]
#p1_cntry = pd.DataFrame(cntry)


#p2 = p1[p1['Location_Country'].notnull()]# DataFrame for not null locations
p2 = p2_final[p2_final['Location_Country'].notnull()]# DataFrame for not null locations
#p3 = p1[p1['Location_Country'].isnull()]# DataFrame for null locations
p3 = p2_final[p2_final['Location_Country'].isnull()]# DataFrame for null locations

#################################################################################


# Step - 4 : Using tzwhere, use the Lats/Longs derived in the last step to determine the Local Time Zone of the region:

from tzwhere import tzwhere
tz = tzwhere.tzwhere()
#print tz.tzNameAt(35.29, -89.66)
#geocoder.timezone(Point(40.7410861, -73.9896297241625), time=datetime.utcnow())

def gc(p2):
	p2['Location_Timezone'] = tz.tzNameAt(p2['Location_Lat'], p2['Location_Long'])
	return p2['Location_Timezone']

p2['Location_Timezone'] = p2.apply(gc, axis=1)
p2.to_csv('Users200k_withTimezones.csv')


###################################################################################

# Step - 5 : Download data from "Posts" table. Join the "Posts" dataset with "Users" dataset (above):

"""
select top 50000 * from Posts
 
   order by id;
   
select top 50000
* from Posts where not in
             (select top 50000 id from Posts
			 order by id)
   order by id;

select top 50000
* from Posts where not in
             (select top 100000 id from Posts
			 order by id);
"""


###################################################################################

# Step - 6 : Using pytz to convert the Post_Creation_Date (in UTC) to regionwise local time (using the resolved value of local timezone for each region):

from datetime import datetime, timedelta
import pytz



p2_temp = pd.read_csv('Post_User-Merge_200k_20k.csv')
p2 = p2_temp.dropna(thresh=2)
p2 = p2[p2.Location_Timezone.notnull()]

p2['Year']=[d.split('-')[0] for d in p2.Post_CreationDate]
p2['Month']=[d.split('-')[1] for d in p2.Post_CreationDate]
p2['Day1']=[d.split('-')[2] for d in p2.Post_CreationDate]
p2['Day']=[d.split(' ')[0] for d in p2.Day1]
p2['Hour1']= [d.split(':')[0] for d in p2.Day1]
p2['Hour']= [d.split(' ')[1] for d in p2.Hour1]
p2['Minute']= [d.split(':')[1] for d in p2.Day1]

p2.Year = p2.Year.astype(np.int64)
p2.Month = p2.Month.astype(np.int64)
p2.Day = p2.Day.astype(np.int64)
p2.Hour = p2.Hour.astype(np.int64)
p2.Minute = p2.Minute.astype(np.int64)


"""
def utc_to_local(Year, Month, Day, Hour, Minute, Timezone):
    utc_dt = datetime(Year, Month, Day, Hour, Minute)
    local_tz = pytz.timezone(Timezone)
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    nlocal_dt = local_tz.normalize(local_dt)
    snlocal_dt = nlocal_dt.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
    return snlocal_dt

for i in range(len(p_temp['CreationDate'])):
    p_temp['Local_CreationDate'][i] = utc_to_local(p_temp.Year[i],p_temp.Month[i],p_temp.Day[i],p_temp.Hour[i],p_temp.Minute[i],p_temp.Location_Timezone[i])
"""


def utc_to_local(pdt):
    Year = pdt['Year']
    Month = pdt['Month']
    Day = pdt['Day']
    Hour = pdt['Hour']
    Minute = pdt['Minute']
    Timezone = pdt['Location_Timezone']
    utc_dt = datetime(Year, Month, Day, Hour, Minute)
    try:
		local_tz = pytz.timezone(Timezone)
	except:
		return 'NaN'
	else:
		local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
		nlocal_dt = local_tz.normalize(local_dt)
		snlocal_dt = nlocal_dt.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
		return snlocal_dt

p2['Post_Local_CreationDate'] = p2.apply(utc_to_local, axis=1)
p2.to_csv('Local_Time_Post_User-Merge_200k_20k.csv')

p2.drop(['Day1', 'Hour1', 'Year', 'Month', 'Day', 'Hour', 'Minute'], axis=1, inplace=True)

