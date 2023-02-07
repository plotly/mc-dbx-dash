    
import os, datetime, random, decimal
import sqlalchemy
from sqlalchemy import create_engine, select, insert, Column, MetaData, Table, BOOLEAN
from sqlalchemy import Table, Column, Date, Integer, String, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, declarative_base, Session
import pandas as pd


  
SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapi39d334f86fe32b3ed9027b79902c68ec"
CATALOG = "main"
SCHEMA = "mc_aggrid_demo"

engine = create_engine(f"databricks+thrift://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}")

session  = Session(bind=engine)
base = declarative_base(bind=engine)


class TableObject(base):

    __tablename__ = "bronze_sensors"

    Plant = Column(Integer, primary_key=True)
    Plant_Name = Column(String(255))
    Brand_Family = Column(String(255))
    Brand_Long_Name = Column(String(255))
    OSKU_Number = Column(Integer)
    Material_Number = Column(Integer)
    Material_Description = Column(String(255))
    Promotion = Column(String(255))
    Base_Flag = Column(String(255))
    Container_Type = Column(String(255))
    Deposit_Labeling = Column(String(255))
    Procurement_Type = Column(String(255))
    Days_To_Ship = Column(Integer)
    Manufacturing_YrWk = Column(String(255))
    Current_Unrestricted_Stock_Units = Column(Integer)
    Current_Quality_Inspection_Units = Column(Integer)
    Current_Blocked_Stock_Units = Column(Integer)
    Loaded_in_Yard = Column(Integer)
    Current_Total_Units = Column(Integer)
    Product_Cost = Column(String(255))
    Import_Flag = Column(String(255))
    Export_Flag = Column(String(255))
    Pabst_Flag = Column(String(255))
    Units_per_Pallet = Column(Integer)
    Type = Column(String(255))
    Category = Column(String(255))
    Reason_Code = Column(String(255))
    Comments = Column(String(255))

base.metadata.create_all()

table_object_1 = TableObject(Plant=1000, Plant_Name="Golden Brewery",
 Brand_Family = "BLUE MOON FAMILY", 
 Brand_Long_Name = "BLUE MOON BELGIAN WHITE ALE", 
 OSKU_Number = 700017, 
 Material_Number = 21950, 
 Material_Description = "BMBW 1/2BBL STD BLY KEG (5)-3", 
 Promotion = "None", 
 Base_Flag = "Y", 
 Container_Type = "KEG", 
 Deposit_Labeling = "ND", 
 Procurement_Type = "INTERNAL", 
 Days_To_Ship = -1,  
 Manufacturing_YrWk = "2022-34",
 Current_Unrestricted_Stock_Units = 9)
session.add(table_object_1)
session.commit()

stmt = select(TableObject).where(TableObject.Plant_Name.in_(["Golden Brewery"]))

output = [i for i in session.scalars(stmt)]
assert len(output) == 1 

base.metadata.drop_all()

stmt1 = f"SELECT * FROM mc_aggrid_demo.bronze_sensors;"

df = pd.read_sql_query(stmt1, engine)





