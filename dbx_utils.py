import os, datetime, random
import sqlalchemy
from sqlalchemy import create_engine, select, insert, Column, MetaData, Table, BOOLEAN
from sqlalchemy import Table, Column, Date, Integer, String, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, declarative_base, Session
import pandas as pd



SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapi39d334f86fe32b3ed9027b79902c68ec"
catalog = "main"
schema = "default"


# engine = create_engine(
#     f"databricks+connector://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}:443/{DBNAME}",
#     connect_args={
#         "http_path": f"{HTTP_PATH}",
#     },
# )

engine = create_engine(f"databricks+thrift://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={catalog}&schema={schema}")

session  = Session(bind=engine)
base = declarative_base(bind=engine)


class SampleObject(base):

    __tablename__ = "mc_aggrid_demo"

    name = Column(String(255), primary_key=True)
    number = Column(Integer)

base.metadata.create_all()

sample_object_1 = SampleObject(name="Bim Adewunmi", number=6)
sample_object_2 = SampleObject(name="Miki Meek", number=12)
session.add(sample_object_1)
session.add(sample_object_2)
session.commit()

stmt1 = select(SampleObject).where(SampleObject.name.in_(["Bim Adewunmi", "Miki Meek"]))

output = [i for i in session.scalars(stmt1)]
assert len(output) == 2

base.metadata.drop_all()

metadata_obj = MetaData(bind= engine)

table_name = "mc_aggrid_demo"
names = ["Bim", "Miki", "Sarah", "Ira"]
rows = [{"name": names[i%3], "number": random.choice(range(10000))} for i in range(10000)]

SampleTable = Table(
        table_name,
        metadata_obj,
        Column("name", String(255)),
        Column("number", Integer)
)

# Create SampleTable ~5 seconds
metadata_obj.create_all()

# Insert 10k rows takes < 3 seconds
engine.execute(insert(SampleTable).values(rows))

results = engine.execute(select(SampleTable)).all()

assert len(results) == 10_000

stmt = f"SELECT * FROM mc_aggrid_demo.bronze_sensors;"

df = pd.read_sql_query(stmt, engine)

