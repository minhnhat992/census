import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import pandas as pd

# connect to census api to pull total population and total poverty by Zip Codes
## set URL
URL = "https://api.census.gov/data/2018/acs/acs5?get=NAME,B01003_001E,B17001_002E&for=zip%20code%20tabulation%20area:*"

## sending get request and saving the response as response object
r = requests.get(url=URL)

## extracting data in json format
data = r.json()

## convert to data frame and change column names
ACS_poverty_population = pd.DataFrame(data)
ACS_poverty_population = ACS_poverty_population.iloc[1:]
ACS_poverty_population.columns = ['Zip_Code_Name', 'Total_Population','Total_Poverty','Zip']


# import zip map
zip_map = pd.read_excel("fact_finder_raw_data/uszips.xlsx",
                        usecols=['zip','city','state_name'])
zip_map.columns = ['Zip', 'City', 'State_Name']



# create postgreSQL tables
db_string = 'postgres+psycopg2://postgres:Maxpayne992#@localhost:5432/census'

db = create_engine(db_string)
base = declarative_base()

class Zip(base):
    __tablename__ = 'Zip'

    Zip = Column(Integer, primary_key=True)
    City = Column(String)
    State_Name = Column(String)

class Census(base):
    __tablename__ = 'Census'

    Zip = Column(Integer, primary_key=True)
    Zip_Code_Name = Column(String)
    Total_Population = Column(Integer)
    Total_Poverty =  Column(Integer)

# establish postgreSQL session and create tables
Session = sessionmaker(db)
session = Session()
base.metadata.create_all(db)

# insert data to postgre sql
ACS_poverty_population.to_sql('Census', con=db, if_exists='append',index=False)
zip_map.to_sql('Zip', con=db, if_exists='append',index=False)

#base.metadata.drop_all(bind=db)


#query data to join Poverty & Population tables
q = session.query(Census.Zip, Census.Total_Population,Census.Total_Poverty,Zip.City,Zip.State_Name).join(Zip, Zip.Zip == Census.Zip).all()

# convert to df, and calculate the Poverty Rate
top_df = pd.DataFrame(q)
top_df['Poverty_rate'] = top_df['Total_Poverty']/top_df['Total_Population']

# Filter for the top 10 most inflicted zip codes by sorting for the most populous but with the highest poverty rate.
top_df = top_df.sort_values(by = ['Total_Population','Poverty_rate'],ascending=False).head(10)
top_df.to_sql("Top_10_Poverty", con=db, if_exists='replace',index=False)



