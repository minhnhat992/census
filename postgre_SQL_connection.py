#test import sql data

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

db_string = 'postgres+psycopg2://postgres:Maxpayne992#@localhost:5432/census'

db = create_engine(db_string)
base = declarative_base()

class Film(base):
    __tablename__ = 'films'

    title = Column(String, primary_key=True)
    director = Column(String)
    year = Column(String)

Session = sessionmaker(db)
session = Session()

base.metadata.create_all(db)

# Create
doctor_strange = Film(title="Doctor Strange", director="Scott Derrickson", year="2016")
session.add(doctor_strange)
session.commit()

# Read
films = session.query(Film)
for film in films:
    print(film.title)

# Update
doctor_strange.title = "Some2016Film"
session.commit()

# Delete
session.delete(doctor_strange)
session.commit()

base.metadata.drop_all(db)

df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})
df.to_sql('users', con=db, if_exists='append')