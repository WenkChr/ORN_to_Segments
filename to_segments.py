import arcpy, os, sys
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor

arcpy.env.overwriteOutput = True

def unique_values(fc, field):
    # Returns a list off unique values for a given field in the unput dataset
    with arcpy.da.SearchCursor(fc, field_names= [field]) as cursor:
        return sorted({row[0] for row in cursor})

def NumberizeField(df, field, numberize_dict):
    # will return a new field in pandas dataframe with the field name + _CDE
    df[field + '_CDE'] = df[field]
    df = df[field + '_CDE'].replace(numberize_dict, inplace= False).asint()
    return df 

#Data Source: https://geohub.lio.gov.on.ca/datasets/mnrf::ontario-road-network-orn-road-net-element
'''                     Conversion Methodology
1.) Get road layer, read into pandas dataframe 
2.) Get list of lookup tables
3.) Create add address range data and join that to the roads dataset
4.) Loop through certain lookup tables and take only those fields that are needed renaming where necessary
'''
#----------------------------------------------------------------------------------------------------------------
#Inputs

directory = os.getcwd()
ORN_GDB = os.path.join(directory, 'Non_Sensitive.gdb')
workingGDB = os.path.join(directory, 'workingGDB.gdb') 
road_ele_data = os.path.join(ORN_GDB, 'ORN_ROAD_NET_ELEMENT')

#----------------------------------------------------------------------------------------------------------------
# Main script

arcpy.env.workspace = ORN_GDB 
tables = arcpy.ListTables()
print('Reading road data into spatial dataframe')
roads_df = pd.DataFrame.spatial.from_featureclass(road_ele_data, dtypes= {'OGF_ID': 'int', 'FROM_JUNCTION_ID':'int', 'TO_JUNCTION_ID': 'int'}) 
OGF_IDS = roads_df.OGF_ID.unique()
# Remove ORN_JUNCTION because it is not applicable 
if 'ORN_JUNCTION' in tables:
    tables.remove('ORN_JUNCTION')
    tables.remove('ORN_BLOCKED_PASSAGE')
    tables.remove('ORN_TOLL_POINT')
    tables.remove('ORN_UNDERPASS')
    tables.remove('ORN_STREET_NAME_PARSED')
    tables.remove('ORN_ADDRESS_INFO')

#Make Address Ranges on L/R
add_rng_df = pd.DataFrame.spatial.from_table(os.path.join(ORN_GDB, 'ORN_ADDRESS_INFO')) # get full dataset
field_prefix = 'ADDRESS_INFO'
add_rng_df.rename(columns={'AGENCY_NAME' : field_prefix + '_AGENCY', 
                        'EFFECTIVE_DATETIME' : field_prefix + '_EFF_DATE',
                        'EVENT_ID' : field_prefix + '_EVENT_ID',
                        }, 
                        inplace= True)
# Select only rows that are in the section currently being worked on
add_rng_df = add_rng_df[add_rng_df['ORN_ROAD_NET_ELEMENT_ID'].isin(OGF_IDS)]
add_rng_base = add_rng_df[['ORN_ROAD_NET_ELEMENT_ID', 
                            field_prefix + '_AGENCY',
                            field_prefix + '_EFF_DATE',
                            field_prefix + '_EVENT_ID',
                            'STREET_SIDE',
                            'HOUSE_NUMBER_STRUCTURE']].drop_duplicates(subset=['ORN_ROAD_NET_ELEMENT_ID'],  keep='first') # Base for adding L/R attributes to the table
print('Calculating Address Range data')
for row in add_rng_df.itertuples():
    index = add_rng_base.ORN_ROAD_NET_ELEMENT_ID[add_rng_base.ORN_ROAD_NET_ELEMENT_ID == row.ORN_ROAD_NET_ELEMENT_ID].index.tolist()[0]
    Structure_CDE = {'Unknown' : -1, 'None' : 0, 'Even' : 1, 'Odd' : 2, 'Mixed' : 3, 'Irregular' : 4}
    
    # Calculate Address range columns and HOUSE_NUMBER_STRUCTURE_CDE based on STREET_SIDE column value
    if row.STREET_SIDE == 'Left':
        add_rng_base.at[index, 'L_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'L_FULL_STREET_NAME'] = row.FULL_STREET_NAME

    if row.STREET_SIDE == 'Right':
        add_rng_base.at[index, 'R_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'R_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'R_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'R_FULL_STREET_NAME'] = row.FULL_STREET_NAME

    if row.STREET_SIDE == 'Both':
        add_rng_base.at[index, 'R_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'R_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'R_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'L_FULL_STREET_NAME'] = row.FULL_STREET_NAME
        add_rng_base.at[index, 'R_FULL_STREET_NAME'] = row.FULL_STREET_NAME

#Merge the Address Range data to the roads data beofre looping other tables 
roads_df = roads_df.merge(add_rng_base, how= 'left', left_on='OGF_ID', right_on= 'ORN_ROAD_NET_ELEMENT_ID')
roads_df = roads_df.drop(['ORN_ROAD_NET_ELEMENT_ID'], axis=1)
print('Adding non address data to table')
for table in tables: #Loop for line tables
    field_prefix = table[4:]
    print(f'Running segmentification on: {table}')
    tbl_df = pd.DataFrame.spatial.from_table(os.path.join(ORN_GDB, table))
    tbl_df = tbl_df.drop(['OBJECTID'], axis=1)
    #Rename Table fields
    tbl_df.rename(columns={'AGENCY_NAME' : field_prefix + '_AGENCY', 
                            'EFFECTIVE_DATETIME' : field_prefix + '_EFF_DATE',
                            'EVENT_ID' : field_prefix + '_EVENT_ID',
                            'NATIONAL_UUID' : field_prefix + '_NAT_UUID', 
                            'STREET_SIDE': field_prefix + '_STREET_SIDE', 
                            'FULL_STREET_NAME': field_prefix + '_FULL_STREET_NAME'}, 
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

#Export the complete roads df
print('Exporting compiled dataset.')
roads_df.spatial.to_featureclass(os.path.join(workingGDB, 'full_test'), overwrite= True)

print('DONE!')
