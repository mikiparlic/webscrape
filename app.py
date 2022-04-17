import pandas as pd
import requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, Integer, String
from os.path import exists

Base = declarative_base()
DATABASE_FILE = 'base.db'
DATABASE_TABLE = 'hollydays'

class Hollyday(Base):
    __tablename__ = DATABASE_TABLE

    id = Column(Integer, primary_key=True)
    year = Column(String)
    hollyday_date = Column(String)
    hollyday_name = Column(String)

    def __repr__(self):
        return f"('{self.year}'," \
               f"'{self.hollyday_date}', " \
               f"{self.hollyday_name})"


engine = create_engine(f'sqlite:///{DATABASE_FILE}')
session = Session(bind=engine)
YEARS = ['2022', '2023']
ENDPOINT = 'https://www.commerce.wa.gov.au/labour-relations/public-holidays-western-australia'

data = []

def send_request(endpoint):
    page_data = requests.get(endpoint)
    table_list = pd.read_html(page_data.text)
    return table_list


def parse_data(table_list):
    hollydays = []
    for item in table_list[2]['Unnamed: 0']:
        hollydays.append(str(item))
    for item in YEARS:
        if item in table_list[2]:
            ind = 0
            for date in table_list[2][item]:
                hollyday = Hollyday(year=item,
                                    hollyday_date=date,
                                    hollyday_name=hollydays[ind])
                data.append(hollyday)
                ind += 1
    return data

def print_data(data):
    print("Printing from memory")
    for item in data:
        print(item)
    print()

def save_to_sql(data):
    for item in data:
        session.add(item)
    session.commit()

def read_data_from_sql(data):
    read_data = session.query(Hollyday).all()
    print("Printing from database read")
    for item in read_data:
        print(item)
    print()

if __name__ == "__main__":
    if not exists(DATABASE_FILE):
        Base.metadata.create_all(engine)
    table_list = send_request(ENDPOINT)
    data = parse_data(table_list)
    print_data(data)
    save_to_sql(data)
    read_data_from_sql(data)

