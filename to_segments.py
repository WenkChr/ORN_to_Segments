import arcpy, os, sys
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor

arcpy.env.overwriteOutput = True

def unique_values(fc, field):
    # Returns a list off unique values for a given field in the unput dataset
    with arcpy.da.SearchCursor(fc, field_names= [field]) as cursor:
        return sorted({row[0] for row in cursor})

#Data Source: https://geohub.lio.gov.on.ca/datasets/mnrf::ontario-road-network-orn-road-net-element
'''                     Conversion Methodology
1.) Get road layer, read into pandas dataframe 
2.) Get list of lookup tables
3.) Loop through lookup tables and take only those fields that are needed 
'''


directory = os.getcwd()
ORN_GDB = os.path.join(directory, 'Non_Sensitive.gdb')
workingGDB = os.path.join(directory, 'workingGDB.gdb') 
road_ele_data = os.path.join(workingGDB, 'ORN_net_element_tester')

arcpy.env.workspace = ORN_GDB 
tables = arcpy.ListTables()

# Remove ORN_JUNCTION because it is not applicable 
if 'ORN_JUNCTION' in tables:
    tables.remove('ORN_JUNCTION')
    tables.remove('ORN_BLOCKED_PASSAGE')
    tables.remove('ORN_TOLL_POINT')
    tables.remove('ORN_UNDERPASS')
    tables.remove('ORN_STREET_NAME_PARSED')


point_data = 
roads_df = pd.DataFrame.spatial.from_featureclass(road_ele_data, dtypes= {'OGF_ID': 'int', 'FROM_JUNCTION_ID':'int', 'TO_JUNCTION_ID': 'int'}) 
for table in tables: #Loop for line tables
    field_prefix = table[4:]
    print(f'Running segmentification on: {table}')
    tbl_df = pd.DataFrame.spatial.from_table(os.path.join(ORN_GDB, table))
    tbl_df = tbl_df.drop(['OBJECTID'], axis=1)
    #Rename Table fields
    tbl_df.rename(columns={'AGENCY_NAME' : field_prefix + '_AGENCY', 
                            'EFFECTIVE_DATETIME' : field_prefix + '_EFF_DATE',
                            'EVENT_ID' : field_prefix + '_EVENT_ID',
                            'NATIONAL_UUID' : field_prefix + '_NAT_UUID'}, 
                            inplace= True)

    tbl_df[field_prefix + '_measure_dif'] = np.where(tbl_df['FROM_MEASURE'] > tbl_df['TO_MEASURE'], 
                        tbl_df['FROM_MEASURE'] - tbl_df['TO_MEASURE'], 
                        tbl_df['TO_MEASURE'] - tbl_df['FROM_MEASURE']) #Get the measure dif value 

    print(f'Roads length: {len(roads_df)} Table Length: {len(tbl_df)}')
    merged = roads_df.merge(tbl_df, how= 'left', left_on='OGF_ID', right_on= 'ORN_ROAD_NET_ELEMENT_ID')
    #Sort measure dif values highest to lowest and then drop duplicate OGF_ID records from the dataframe. Leaving only the largest dif (longest seg)
    merged.sort_values(by=[field_prefix + '_measure_dif'], ascending= True)
    merged = merged.drop_duplicates(subset=['OGF_ID'], keep='first')
    merged.astype({'OGF_ID' : int})
    print('Length of dataframe: ' + str(len(merged)))
    merged = merged.drop( [field_prefix + '_measure_dif', 
                        'ORN_ROAD_NET_ELEMENT_ID', 
                        'FROM_MEASURE', 
                        'TO_MEASURE'],
                         axis=1) # Removes non-essential fields
    roads_df = merged


for f in roads_df.columns:
    print(f)

#Export the complete roads df
arcpy.env.workspace = workingGDB
roads_df.spatial.to_featureclass('full_test', overwrite= True)

# Narrow data to current zone
#ORN_OGF_IDs = unique_values(road_ele_data, 'OGF_ID')

print('DONE!')
