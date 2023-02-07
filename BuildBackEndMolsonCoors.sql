
CREATE DATABASE IF NOT EXISTS mc_aggrid_demo;
USE mc_aggrid_demo;

CREATE TABLE IF NOT EXISTS mc_aggrid_demo.bronze_sensors

(
Plant INT, 
Plant_Name STRING, 
Brand_Family STRING, 
Brand_Long_Name STRING, 
OSKU_Number INT, 
Material_Number INT, 
Material_Description STRING, 
Promotion STRING, 
Base_Flag STRING, 
Container_Type STRING, 
Deposit_Labeling STRING, 
Procurement_Type STRING, 
Days_To_Ship INT, 
Manufacturing_YrWk STRING, 
Manufacturing_Date DATE, 
Shelf_Life_Expiration DATE, 
STW_by_Date DATE, 
Current_Unrestricted_Stock_Units INT, 
Current_Quality_Inspection_Units INT, 
Current_Blocked_Stock_Units INT, 
Loaded_in_Yard INT, 
Current_Total_Units INT, 
Product_Cost DECIMAL(10,2),
Import_Flag STRING, 
Export_Flag STRING, 
Pabst_Flag STRING, 
Units_per_Pallet INT, 
Type STRING, 
Category STRING, 
Reason_Code STRING, 
Comments STRING
)
USING DELTA 
TBLPROPERTIES("delta.targetFileSize"="128mb")
;

COPY INTO mc_aggrid_demo.bronze_sensors
FROM (SELECT 
_c0::integer AS Plant, 
_c1 as Plant_Name, 
_c2 as Brand_Family,
_c3 as Brand_Long_Name,
_c4::integer AS OSKU_Number,
_c5::integer as Material_Number,
_c6 as Material_Description, 
_c7 as Promotion,
_c8 as Base_Flag,
_c9 as Container_Type, 
_c10 as Deposit_Labeling, 
_c11 as Procurement_Type, 
_c12::integer as Days_To_Ship, 
_c13 as Manufacturing_YrWk, 
_c14::date as Manufacturing_Date, 
_c15::date as Shelf_Life_Expiration, 
_c16::date as STW_by_Date,
_c17::integer as Current_Unrestricted_Stock_Units, 
_c18::integer as Current_Quality_Inspection_Units, 
_c19::integer as Current_Blocked_Stock_Units, 
_c20::integer as Loaded_in_Yard, 
_c21::integer as Current_Total_Units, 
_c22::DECIMAL(10,2) AS Product_Cost, 
_c23 as Import_Flag, 
_c24 as Export_Flag, 
_c25 as Pabst_Flag, 
_c26::integer as Units_per_Pallet, 
_c27 as Type, 
_c28 as Category, 
_c29 as Reason_Code, 
_c30 as Comments
FROM "/FileStore/plotlydata/Coors_Sample_Data___sled_report_example_wk_38.csv"
)
FILEFORMAT = csv
COPY_OPTIONS('force'='true')

CREATE DATABASE IF NOT EXISTS mc_aggrid_demo;
USE mc_aggrid_demo;

CREATE TABLE IF NOT EXISTS mc_aggrid_demo.write_back
(
Plant INT, 
Plant_Name STRING, 
Brand_Family STRING, 
Brand_Long_Name STRING, 
OSKU_Number INT, 
Material_Number INT, 
Material_Description STRING, 
Promotion STRING, 
Base_Flag STRING, 
Container_Type STRING, 
Deposit_Labeling STRING, 
Procurement_Type STRING, 
Days_To_Ship INT, 
Manufacturing_YrWk STRING, 
Current_Unrestricted_Stock_Units INT, 
Current_Quality_Inspection_Units INT, 
Current_Blocked_Stock_Units INT, 
Loaded_in_Yard INT, 
Current_Total_Units INT, 
Product_Cost DECIMAL(10,2),
Import_Flag STRING, 
Export_Flag STRING, 
Pabst_Flag STRING, 
Units_per_Pallet INT, 
Type STRING, 
Category STRING, 
Reason_Code STRING, 
Comments STRING
)
USING DELTA 
TBLPROPERTIES("delta.targetFileSize"="128mb")