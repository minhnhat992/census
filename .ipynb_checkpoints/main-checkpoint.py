import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pprint as pp

#due to api key techincal error, had to manually pull raw data from American Fact Finder
# pulling list of zip codes, total population by zip, population classfifed as poverty by zip
# import data
ACS_Poverty = pd.read_csv("fact_finder_raw_data/ACS_17_5YR_S1701_with_ann.csv", header=1,
                          usecols = ["Id","Geography","Below poverty level; Estimate; Population for whom poverty status is determined"])
ACS_Poverty.columns = ['Id', 'Geography','Total_Poverty']

ACS_Zip = pd.read_csv("fact_finder_raw_data/ACS_17_5YR_G001_with_ann.csv", header=1,
                      usecols = ["Id","Geography"])

ACS_Population = pd.read_csv("fact_finder_raw_data/ACS_17_5YR_B01003_with_ann.csv", header=1,
                             usecols = ["Id","Geography","Estimate; Total"])
ACS_Population.columns = ['Id', 'Geography','Total_Population']


#create postgreSQL tables
db_string = 'postgres+psycopg2://postgres:Maxpayne992#@localhost:5432/census'

db = create_engine(db_string)
base = declarative_base()

class Zip(base):
    __tablename__ = 'Zip'

    Id = Column(String, primary_key=True)
    Geography = Column(String)

class Population(base):
    __tablename__ = 'Population'

    Id = Column(String, primary_key=True)
    Geography = Column(String)
    Total_Population =  Column(Integer)

class Poverty(base):
    __tablename__ = 'Poverty'

    Id = Column(String, primary_key=True)
    Geography = Column(String)
    Total_Poverty = Column(Integer)

Session = sessionmaker(db)
session = Session()
base.metadata.create_all(db)

# insert data to postgre sql
ACS_Poverty.to_sql('Poverty', con=db, if_exists='append',index=False)
ACS_Population.to_sql('Population', con=db, if_exists='append',index=False)
ACS_Zip.to_sql('Zip', con=db, if_exists='append',index=False)
#base.metadata.drop_all(bind=db)


#query data to join Poverty & Population tables
q = session.query(Population.Id, Population.Geography,
                  Population.Total_Population,Poverty.Total_Poverty).join(Poverty, Population.Id == Poverty.Id).all()

# convert to df, and calculate the Poverty Rate
test_df = pd.DataFrame(q)
test_df['Poverty_rate'] = test_df['Total_Poverty']/test_df['Total_Population']

# Filter for the top 10 most inflicted zip codes by sorting for the most populous but with the highest poverty rate.
test_df = test_df.sort_values(by = ['Total_Population','Poverty_rate'],ascending=False).head(10)
test_df.to_sql("Top_10_Poverty", con=db, if_exists='replace',index=False)



