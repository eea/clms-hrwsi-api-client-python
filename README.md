# Python client for accessing the Copernicus Land Monitoring Service - High Resolution Water, Snow & Ice (HR-WSI) products

## Description
Python client for accessing the Copernicus Land Monitoring Service HR-WSI data by means of the S3 AWS tools. A full list of the available data can be found [here](https://s3.waw3-2.cloudferro.com/swift/v1/HRWSI/).

This script allows to easily download HR-WSI products over the EEA38+UK area:
#### Near real-time and daily products
+ FSC: Fractional Snow Cover
+ SWS: SAR Wet Snow
+ GFSC: Gap-filled Fractional Snow Cover
+ WDS: Wet/Dry Snow
+ CC: Cloud Classification
+ WIC_S1: Water/Ice Cover from Sentinel-1 data
+ WIC_S2: Water/Ice Cover from Sentinel-2 data
+ WIC_S1S2: Water/Ice Cover from the merge of WIC_S1 and WIC_S2
\
Visit the CLMS portal land.copernicus.eu for more details on the products (documentation, production or dissemination updates).

This client allows to reach all the available pan-European HR-WSI products, apart from the AWIC ice data which is reachable through the Python client https://github.com/eea/clms-hrwsi-api-client-python-awic. 

## Contact
Copernicus Land Monitoring Service service desk: https://land.copernicus.eu/en/contact-service-helpdesk

## Installation
[Prerequiste] Install Conda (https://docs.conda.io/projects/conda/en/stable/user-guide/install/) 

A Python environment is needed with the packages described in the _env.yaml_ file:
```S
conda env create -f env.yaml # to create a new Python environment with Conda
```
The environment is called _hrwsi_s3client_. It has to be activated. 
```S
conda activate hrwsi_s3client
```
The environment can be removed with `conda deactivate hrwsi_s3client` and called later with `conda activate hrwsi_s3client`.

Download the Python script and run (use correct path):
```S
python s3_hrwsi_downloader.py --help
```
It works by calling the script as such: `python s3_hrwsi_downloader.py path/to/output/dir [input arguments]`

## Use
The input arguments are:

#### The mode of execution (can only pick one):
-query: for searching without downloading\
-query_and_download: for searching and downloading\
-download: for downloading from a list of products previously generated with -query

#### The mode of selection (can only pick one):
-wkt: Well Known Text (between \"\") describing either a polygon (ex: _\"POLYGON ((1 1,5 1,5 5,1 5,1 1))\"_ ) or a multi polygon (ex: _\"MULTIPOLYGON (((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2)),((6 3,9 2,9 4,6 3)))\")_. For example, WKT can be created online using such a tool: https://wktmap.com/.\
-vector: Vector file containing a 2D vector layer (polygon or multipolygon). Can be `.shp`, `.geosjson`, `.gpkg`, `.kml`. Must include a projection system.\
-tiles: one or more tile identifiers defining the product locations on the Military Grid Reference System (MGRS) grid used for HR-WSI products. Format _T##XXX_ or _##XXX_. More details below.

#### The query parameters:
-epsg: projection system ID. Mandatory if -wkt given. Ex: 4326, 3035 or 32631\
-productType: one or more product types\
-dateStart: start date of the search window. Format _YYYY-MM-DD_.\
-dateEnd: end date of the search window. Format _YYYY-MM-DD_.

#### The download parameter:
-query_file: path to the txt file listing the products found by the query

### The tiling system
The script comes with a vector file _MGRS_tiles.gpkg_ containing all the MGRS tiles used for the tiling of Sentinel-2 optical satellite data. The tiles are provided in the EPSG:4326 coordinate reference system. HR-WSI raster data follows the same tiling convention.
More information on the tiling system can be found at https://hls.gsfc.nasa.gov/products-description/tiling-system/. 
This `.gpkg` file is necessary to dispense the user from knowing the tiling nomenclature, and must be present in the same directory as the Python script. 
However, its presence is not necessary if the script is used solely to download products from a predefined list, or if the user manually provides the tile identifiers as input.

### Download threshold
The download is limited to 500 products per run.

### Output organisation
<pre>
output_folder
    |query_file.txt
    |results
        |product1
        |product2
            :
        |productN
            |layer1
            |layer2
               :
</pre>              
  
## Examples 
```S
python s3_hrwsi_downloader.py output_folder -query -productType FSC WIC_S2 -tiles T31TCH T30TYN -dateStart 2025-02-01 -dateEnd 2025-02-15\
python s3_hrwsi_downloader.py output_folder -query_and_download -productType FSC -wkt "POLYGON ((704922.894694 4756709.422481, 920001.318865 4729607.8903, 704922.894694 4756709.422481))" -epsg 32630 -dateStart 2025-02-01 -dateEnd 2025-02-15\
python s3_hrwsi_downloader.py output_folder -query_and_download -productType SWS -vector path/to/layer.shp -dateStart 2025-02-15 -dateEnd 2025-03-15\
python s3_hrwsi_downloader.py output_folder -download -query_file query_file.txt
```

## Legal notice about Copernicus Data
Access to data is based on a principle of full, open and free access as established by the Copernicus data and information policy Regulation (EU) No 1159/2013 of 12 July 2013. This regulation establishes registration and licensing conditions for GMES/Copernicus users and can be found here: http://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32013R1159.  

Free, full and open access to this data set is made on the conditions that:  
1. When distributing or communicating Copernicus dedicated data and Copernicus service information to the public, users shall inform the public of the source of that data and information.  
2. Users shall make sure not to convey the impression to the public that the user's activities are officially endorsed by the Union.  
3. Where that data or information has been adapted or modified, the user shall clearly state this.  
4. The data remain the sole property of the European Union. Any information and data produced in the framework of the action shall be the sole property of the European Union. Any communication and publication by the beneficiary shall acknowledge that the data were produced “with funding by the European Union”.  





