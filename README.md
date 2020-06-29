# ORN_to_Segments

This project uses python to convert the Ontario Road Network (ORN) Linear Referencing System (LRS) into road segments for the purpose of integration into the National Road Network (NRN). The original ORN LRS can be found here: https://geohub.lio.gov.on.ca/datasets/mnrf::ontario-road-network-orn-road-net-element 

Steps to process the ORN LRS into road segments:

1.) Download and unzip the data from the source. Use the GIS software of your choice to convert the ORN_TOLL_BOOTHS and the ORN_BLOCKED_PASSAGES into point files. Note that if you are doing this in ArcGIS you qill be required to have the Network Analyst extension installed for this to work. It is also possible to complete this task using QGIS utilizing the 'LRS' plugin.

2.) Run either to_segments.py or to_segments_gpd.py. Both files produce the same results however they utilize different python packages to produce their outputs. the to_segments.py requires an ArcGIS Pro environment as it relies on arcpy and the arcgis API in the data prcessing. The to_segments_gpd.py uses open source geopandas and as such requires a working Python 3.6 environment with the goepandas installed. 

3.) Fill in the YAML files one for each NRN data product documentation and descriptions of each field can be found here: https://nrn-rrn.readthedocs.io/en/latest/feature_catalog.html.#

Description of segmentation process:

There is no perfect solution for getting an LRS into a simple line segment file some granularity in the data will always be lost due to the overlapping nature of a LRS. The base case solution here was to look at every road segments and its associated tables and take the attribute that covered the longest portion of that line segment. So for non address fields such as pavement surface or number of lanes the difference between the FROM_MEASURE and the TO_MEASURE fields are calculated and the section that covers the longest distance over that road segment is chosen as the value that will be used in the segmented version of the data.  
