import os, sys
import pandas as pd
import numpy as np
import geopandas as gpd
import fiona
def NumberizeField(df, field, outFieldName, numberize_dict):
    # will return a new field in pandas dataframe with the field name + _CDE
    df.insert(df.columns.get_loc(field) + 1, outFieldName + '_CDE', df[field])
    df[outFieldName + '_CDE'] = df[field].map(numberize_dict)
    pd.to_numeric(df[outFieldName + '_CDE'], errors= 'ignore')
    
#Data Source: https://geohub.lio.gov.on.ca/datasets/mnrf::ontario-road-network-orn-road-net-element
'''                     Conversion Methodology
Note that the blocked passages and toll point data is excluded from this methodology because the tool needed in arcgis was not licenced.
These data points were created in QGIS instead 
1.) Get road layer, read into pandas dataframe 
2.) Get list of lookup tables
3.) Create add address range data and join that to the roads dataset
4.) Loop through certain lookup tables and take only those fields that are needed renaming where necessary
5.) While looping if a line segment has multiple associated events take the event data that covers the longest part of the segment
'''
#----------------------------------------------------------------------------------------------------------------
#Inputs

directory = os.getcwd()
ORN_GDB = os.path.join(directory, 'Non_Sensitive.gdb')
workingGDB = os.path.join(directory, 'workingGDB.gdb')
outGDB = os.path.join(directory, 'files_for_delivery.gdb') 
road_ele_data = os.path.join(ORN_GDB, 'ORN_ROAD_NET_ELEMENT') # Full road dataset
#road_ele_data = os.path.join(workingGDB, 'ORN_net_element_tester') #Test area road dataset

#----------------------------------------------------------------------------------------------------------------
# Main script

tables = fiona.listlayers(ORN_GDB) # List of everything in ORN_GDB
print('Reading road data into spatial dataframe')
#roads_df = pd.DataFrame.spatial.from_featureclass(road_ele_data, dtypes= {'OGF_ID': 'int', 'FROM_JUNCTION_ID':'int', 'TO_JUNCTION_ID': 'int'}) 
roads_df = gpd.read_file(workingGDB, layer='ORN_net_element_tester')
OGF_IDS = roads_df.OGF_ID.unique()
# Remove tables and fc's that require specific treatment or are not required 
for tbl in ['ORN_ROAD_NET_ELEMENT', 'ORN_JUNCTION', 'ORN_BLOCKED_PASSAGE', 'ORN_TOLL_POINT', 'ORN_UNDERPASS', 'ORN_STREET_NAME_PARSED', 'ORN_ADDRESS_INFO', 'ORN_ROUTE_NAME', 'ORN_ROUTE_NUMBER', 'ORN_OFFICIAL_STREET_NAME']:
    tables.remove(tbl)

#Make Address Ranges on L/R
add_rng_df = gpd.read_file(ORN_GDB, layer='ORN_ADDRESS_INFO') # get full dataset
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

#Merger of street nme table
str_nme_parsed_df = gpd.read_file(ORN_GDB, layer='ORN_STREET_NAME_PARSED') # ORN_STREET_NAME_PARSED table as a df
add_rng_df = add_rng_df.merge(str_nme_parsed_df, how= 'left', left_on='FULL_STREET_NAME', right_on= 'FULL_STREET_NAME')

#Add the columns to be calculated below as empty columns but specify te dtypes so that there are no errors
temp_df = pd.DataFrame({'L_HOUSE_NUMBER_STRUCTURE_CDE' : pd.Series([], dtype='int'), 
                        'L_FIRST_HOUSE_NUM' : pd.Series([], dtype='int'),
                        'L_LAST_HOUSE_NUM' : pd.Series([], dtype='int'),
                        'L_FULL_STREET_NAME' :  pd.Series([], dtype='str'), 
                        'L_DIR_PRE' : pd.Series([], dtype= 'str'),
                        'L_STR_TYP_PRE' : pd.Series([], dtype= 'str'),
                        'L_STR_NME_BDY' : pd.Series([], dtype= 'str'),
                        'L_STR_TYP_SUF' : pd.Series([], dtype= 'str'),
                        'L_DIR_SUF' : pd.Series([], dtype= 'str'),
                        'R_HOUSE_NUMBER_STRUCTURE_CDE' : pd.Series([], dtype='int'), 
                        'R_FIRST_HOUSE_NUM' : pd.Series([], dtype='int'),
                        'R_LAST_HOUSE_NUM' : pd.Series([], dtype='int'),
                        'R_FULL_STREET_NAME' :  pd.Series([], dtype='str'), 
                        'R_DIR_PRE' : pd.Series([], dtype= 'str'),
                        'R_STR_TYP_PRE' : pd.Series([], dtype= 'str'),
                        'R_STR_NME_BDY' : pd.Series([], dtype= 'str'),
                        'R_STR_TYP_SUF' : pd.Series([], dtype= 'str'),
                        'R_DIR_SUF' : pd.Series([], dtype= 'str')
                        })
add_rng_base = pd.concat([add_rng_base, temp_df], axis= 1) # add the new fields to addr_rng_base via a concat
#Loop to add address info to roads df
print('Calculating Address Range data')
for row in add_rng_df.itertuples():
    index = add_rng_base.ORN_ROAD_NET_ELEMENT_ID[add_rng_base.ORN_ROAD_NET_ELEMENT_ID == row.ORN_ROAD_NET_ELEMENT_ID].index.tolist()[0]
    Structure_CDE = {'Unknown' : -1, 'None' : 0, 'Even' : 1, 'Odd' : 2, 'Mixed' : 3, 'Irregular' : 4}
    # Calculate Address range columns and HOUSE_NUMBER_STRUCTURE_CDE based on STREET_SIDE column value
    if row.STREET_SIDE == 'Left':
        add_rng_base.at[index, 'L_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'L_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_FULL_STREET_NAME'] = row.FULL_STREET_NAME
        #STREET NAME PARSED data
        add_rng_base.at[index, 'L_DIR_PRE'] = row.DIRECTIONAL_PREFIX
        add_rng_base.at[index, 'L_STR_TYP_PRE'] = row.STREET_TYPE_PREFIX
        add_rng_base.at[index, 'L_STR_NME_BDY'] = row.STREET_NAME_BODY
        add_rng_base.at[index, 'L_STR_TYP_SUF'] = row.STREET_TYPE_SUFFIX
        add_rng_base.at[index, 'L_DIR_SUF'] = row.DIRECTIONAL_SUFFIX

    if row.STREET_SIDE == 'Right':
        add_rng_base.at[index, 'R_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'R_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'R_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'R_FULL_STREET_NAME'] = row.FULL_STREET_NAME
        #STREET NAME PARSED data
        add_rng_base.at[index, 'R_DIR_PRE'] = row.DIRECTIONAL_PREFIX
        add_rng_base.at[index, 'R_STR_TYP_PRE'] = row.STREET_TYPE_PREFIX
        add_rng_base.at[index, 'R_STR_NME_BDY'] = row.STREET_NAME_BODY
        add_rng_base.at[index, 'R_STR_TYP_SUF'] = row.STREET_TYPE_SUFFIX
        add_rng_base.at[index, 'R_DIR_SUF'] = row.DIRECTIONAL_SUFFIX

    if row.STREET_SIDE == 'Both':
        add_rng_base.at[index, 'L_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'R_HOUSE_NUMBER_STRUCTURE_CDE'] = Structure_CDE[row.HOUSE_NUMBER_STRUCTURE]
        add_rng_base.at[index, 'L_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'R_FIRST_HOUSE_NUM'] = row.FIRST_HOUSE_NUMBER
        add_rng_base.at[index, 'R_LAST_HOUSE_NUM'] = row.LAST_HOUSE_NUMBER
        add_rng_base.at[index, 'L_FULL_STREET_NAME'] = row.FULL_STREET_NAME
        add_rng_base.at[index, 'R_FULL_STREET_NAME'] = row.FULL_STREET_NAME
        #STREET NAME PARSED data
        add_rng_base.at[index, 'L_DIR_PRE'] = row.DIRECTIONAL_PREFIX
        add_rng_base.at[index, 'L_STR_TYP_PRE'] = row.STREET_TYPE_PREFIX
        add_rng_base.at[index, 'L_STR_NME_BDY'] = row.STREET_NAME_BODY
        add_rng_base.at[index, 'L_STR_TYP_SUF'] = row.STREET_TYPE_SUFFIX
        add_rng_base.at[index, 'L_DIR_SUF'] = row.DIRECTIONAL_SUFFIX
        add_rng_base.at[index, 'R_DIR_PRE'] = row.DIRECTIONAL_PREFIX
        add_rng_base.at[index, 'R_STR_TYP_PRE'] = row.STREET_TYPE_PREFIX
        add_rng_base.at[index, 'R_STR_NME_BDY'] = row.STREET_NAME_BODY
        add_rng_base.at[index, 'R_STR_TYP_SUF'] = row.STREET_TYPE_SUFFIX
        add_rng_base.at[index, 'R_DIR_SUF'] = row.DIRECTIONAL_SUFFIX

#Merge the Address Range data to the roads data beofre looping other tables 
roads_df = roads_df.merge(add_rng_base, how= 'left', left_on='OGF_ID', right_on= 'ORN_ROAD_NET_ELEMENT_ID')
roads_df = roads_df.drop(['OBJECTID', 'ORN_ROAD_NET_ELEMENT_ID', 'HOUSE_NUMBER_STRUCTURE', 
                        'LENGTH','STREET_SIDE', 'FROM_JUNCTION_ID', 'TO_JUNCTION_ID'], axis=1)

print('Adding non address data to table')
for table in tables: #Loop for line tables
    field_prefix = table[4:]
    print(f'Running segmentification on: {table}')
    tbl_df = gpd.read_file(ORN_GDB, layer=table)
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
                        field_prefix + '_EVENT_ID',
                        'FROM_MEASURE',
                        'TO_MEASURE', 
                        'ORN_ROAD_NET_ELEMENT_ID'],
                         axis=1) # Removes non-essential fields
    roads_df = merged
print('Creating route name and number multi fields')
#Route Name and Number Multifields
for table in ['ORN_ROUTE_NUMBER', 'ORN_ROUTE_NAME']:
    field_prefix = table[4:]
    print(f'Running segmentification on: {table}')
    tbl_df = gpd.read_file(ORN_GDB, layer=table)
    tbl_df = tbl_df.drop(['OBJECTID', 'EVENT_ID', 'AGENCY_NAME'], axis=1) # Drop some excess fields
    print(f'iterating over matches for the ORN_NET_ID in {table}')
    for row in roads_df.itertuples():
        ogf_id = row.OGF_ID
        matched_df = tbl_df.loc[tbl_df['ORN_ROAD_NET_ELEMENT_ID'] == ogf_id] # df of compiled matches
        for match_row in matched_df.itertuples():
            fields_to_extract = {'ORN_ROUTE_NUMBER' : ['ROUTE_NUMBER'], 'ORN_ROUTE_NAME' : ['ROUTE_NAME_ENGLISH', 'ROUTE_NAME_FRENCH']}
            count = 1           
            if table == 'ORN_ROUTE_NUMBER':
                roads_df[f'ROUTE_NUMBER_{count}'] = matched_df.ROUTE_NUMBER
                count += 1            
            if table == 'ORN_ROUTE_NAME':
                roads_df[f'ROUTE_NAME_ENGLISH_{count}'] = matched_df.ROUTE_NAME_ENGLISH
                roads_df[f'ROUTE_NAME_FRENCH_{count}'] = matched_df.ROUTE_NAME_FRENCH
                count += 1

print('Encoding select fields from strings into NRN numeric codes')
# Encode certain fields from the loop
NumberizeField(roads_df, 'ACQUISITION_TECHNIQUE', 'ACQUISITION_TECHNIQUE', {'UNKNOWN' : -1,
                                                                        'NONE' : 0,
                                                                        'OTHER' : 1,
                                                                        'GPS' : 2,
                                                                        'ORTHOIMAGE' : 3,
                                                                        'ORTHOPHOTO' : 4,
                                                                        'VECTOR DATA' : 5,
                                                                        'PAPER MAP' : 6,
                                                                        'FIELD COMPLETION' : 7,
                                                                        'RASTER DATA' : 8,
                                                                        'DIGITAL ELEVATION MODEL' : 9,
                                                                        'AERIAL PHOTO' : 10,
                                                                        'RAW IMAGERY DATA' : 11,
                                                                        'COMPUTED' : 12})

NumberizeField(roads_df, 'ROAD_CLASS', 'ROAD_CLASS', {'Freeway' : 1, 
                                                    'Expressway / Highway' : 2, 
                                                    'Arterial' : 3, 
                                                    'Collector' : 4,
                                                    'Local / Street' : 5,
                                                    'Local / Strata' : 6,
                                                    'Local / Unknown' : 7,
                                                    'Alleyway / Laneway' : 8,
                                                    'Ramp' : 9,
                                                    'Resource / Recreation' : 10,
                                                    'Rapid Transit' : 11,
                                                    'Service' : 12,
                                                    'Winter' : 13})   

NumberizeField(roads_df, 'STRUCTURE_TYPE', 'STRUCTURE_TYPE', {'None' : 0,
                                                            'Bridge' : 1,
                                                            'Bridge Covered' : 2,
                                                            'Bridge Moveable' : 3,
                                                            'Tunnel'  : 5,
                                                            'Dam' : 7})

NumberizeField(roads_df, 'DIRECTION_OF_TRAFFIC_FLOW', 'DIRECTION_OF_TRAFFIC_FLOW', 
                                                        {'Unknown' : -1, 
                                                        'Both' : 1,
                                                        'Positive' : 2, # 'Positive' - flow of traffic same as digitizing direction (Same Direction)
                                                        'Negative' : 3 # 'Negative' - flow of traffic different from digitizing direction (Opposite Direction)
                                                        })

NumberizeField(roads_df, 'PAVEMENT_STATUS', 'PAVEMENT_STATUS', {'Paved' : 1, 'Unpaved' : 2})

NumberizeField(roads_df, 'SURFACE_TYPE', 'UNPAVED_SURFACE_TYPE', {'Unknown' : -1,
                                                                'None' : 0,
                                                                'Gravel' : 1,
                                                                'Dirt' : 2})

NumberizeField(roads_df, 'SURFACE_TYPE', 'PAVED_SURFACE_TYPE', {'Unknown' : -1, 
                                                                'None' : 0,
                                                                'Rigid' : 1, # Rigid = Summer (Same def between docs)
                                                                'Flexible' : 2, # Flexible = Winter (Same def between docs)
                                                                'Blocks' : 3
                                                                })

# Directional prefix and suffix encoding
direction_cde = {'None' : 0, 'North' : 1, 'Nord' : 2, 'South' : 3, 'Sud' : 4, 'East' : 5, 'Est' : 6, 'West' : 7, 'Ouest' : 8, 'North West' : 9,
            'Nord Ouest' : 10, 'North East' : 11, 'Nord Est' : 12, 'South West' :13, 'Sud Ouest' : 14, 'South East' : 15, 'Sud Est' : 16, 
            'Central' : 17, 'Centre' : 18}

NumberizeField(roads_df, 'L_DIR_PRE', 'L_DIR_PRE', direction_cde)
NumberizeField(roads_df, 'L_DIR_SUF', 'L_DIR_SUF', direction_cde)

final_field_order = ['OGF_ID', 'NATIONAL_UUID', 'ROAD_ELEMENT_TYPE', 'ACQUISITION_TECHNIQUE', 'ACQUISITION_TECHNIQUE_CDE', 'CREATION_DATE', 
'REVISION_DATE', 'EXIT_NUMBER', 'L_FIRST_HOUSE_NUM', 'R_FIRST_HOUSE_NUM', 'L_HOUSE_NUMBER_STRUCTURE_CDE', 'R_HOUSE_NUMBER_STRUCTURE_CDE', 
'L_LAST_HOUSE_NUM', 'R_LAST_HOUSE_NUM', 'ROAD_CLASS', 'ROAD_CLASS_CDE', 'NUMBER_OF_LANES', 'L_FULL_STREET_NAME', 'R_FULL_STREET_NAME', 
'ALTERNATE_STREET_NAME_FULL_STREET_NAME', 'ALTERNATE_STREET_NAME_EFF_DATE', 'SURFACE_TYPE', 'PAVED_SURFACE_TYPE_CDE', 'PAVEMENT_STATUS', 
'PAVEMENT_STATUS_CDE', 'JURISDICTION', 'JURISDICTION_AGENCY', 'ROUTE_NAME_ENGLISH_1', 'ROUTE_NAME_FRENCH_1', 'ROUTE_NUMBER_1', 'SPEED_LIMIT', 
'STRUCTURE_NAME_ENGLISH', 'STRUCTURE_NAME_FRENCH', 'STRUCTURE_TYPE', 'STRUCTURE_TYPE_CDE', 'DIRECTION_OF_TRAFFIC_FLOW', 
'DIRECTION_OF_TRAFFIC_FLOW_CDE', 'UNPAVED_SURFACE_TYPE_CDE', 'SHAPE']

roads_df = roads_df[final_field_order]

#Export the complete roads df
print('Exporting compiled dataset.')
roads_df.to_file(os.path.join(directory, 'files_for_delivery.gpkg'), layer= 'ORN_Road_Segments', driver='GPKG')

sys.exit()
#Toll Points field encoding
print('Importing and encoding Toll Points data')
tp_df = gpd.read_file(workingGDB, layer= 'ORN_Toll_Points') # ORN_Toll_Points created in QGIS with the linear referencing plugin
tp_df = tp_df.drop(['EVENT_ID', 'AT_MEASURE', 'lrs_err'], axis=1)
NumberizeField(tp_df, 'TOLL_POINT', 'TOLL_PNT_TYP', {'Unknown' :-1, 'Physical' : 1, 'Virtual' : 2, 'Hybrid' : 3})
tp_df.to_file(os.path.join(directory, 'files_for_delivery.gdb'), layer='ORN_toll_booths', driver='GPKG')

#Blocked Passages field encoding
print('Importing and encoding Blocked Passages data')
bp_df = gpd.read_file(workingGDB, layer='ORN_BLocked_Passages')
bp_df = bp_df.drop(['EVENT_ID', 'AT_MEASURE', 'lrs_err'], axis=1)
NumberizeField(bp_df, 'BLOCKED_PA', 'BLKD_PASS_TYP', {'Unknown' : -1, 'Permanent' : 1, 'Removable' : 2})
bp_df.to_file(os.path.join(directory, 'files_for_delivery.gdb'), layer= 'ORN_blocked_passages', driver='GPKG')

print('DONE!')
