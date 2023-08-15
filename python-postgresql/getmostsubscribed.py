import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def connect_db(database):
    user = 'postgres'
    passwd = 'postgres'
    hostname = 'localhost'

    conn_string = f'postgresql://{user}:{passwd}@{hostname}:5432/{database}'
    
    
    db = create_engine(conn_string)
    
    conn = db.connect()
    print("Successfully Connected")

    return conn
    

def most_subscribed(sql):
    # connect to raw database
    conn = connect_db('youtubeglobalstats')
    # read sql table
    data = pd.read_sql(sql, con=conn)
    # connect to datawarehouse
    conn = connect_db('youtubedw')
    # load dataframe to warehouse
    data.to_sql('Subscribers_Number_ByYears', con=conn, if_exists='replace', index=False)

    print('Success')
    return data
    

if __name__ == '__main__':
        sql = """
        SELECT ytb."Youtuber", ytb.subscribers FROM youtubeglobalstats  AS ytb 
        ORDER BY subscribers DESC LIMIT 100;"""
        
        sql_year_2011_2013 = """
        SELECT ytb."Youtuber", ytb.subscribers, ytb.created_year FROM youtubeglobalstats AS 
        ytb WHERE created_year BETWEEN 2011 AND 2013 Limit 100;"""
        
        most_subscribed(sql_year_2011_2013)