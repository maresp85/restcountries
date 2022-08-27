import hashlib
import json
import pandas as pd
import requests
import time
import typing

from database import connection_db, query_statement, save_data


def getAPI(url: str) -> dict: 
    """ connect to API resource
        :param 
            url: URL API
        :return: 
            API Response
    """ 
    response = None
    try:
        response = requests.get(url)
        return response
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


def buildData(content: dict) -> dict:   
    """ Process API response
        :param 
            url: content of response API
        :return: 
            Dict
    """  
    region = []
    city_name = []
    language = []
    elapsed_time = [] 
    for row in content:
        st = time.time()
        region.append(row['region'])
        city_name.append(row['name']['common'])
        if 'languages' in row:
            lang = list(row['languages'].values())[0]
            hash_object = hashlib.sha256(lang.encode('utf-8'))
            language.append(hash_object.hexdigest())            
        else:
            language.append(None)

        et = time.time()
        diff_time = (et - st) * 1000
        elapsed_time.append(diff_time)

    data = {
        'Region': region,
        'City Name': city_name,
        'Language': language,
        'Time (ms)': elapsed_time,
    }

    return data


def operations(df: typing.Union[str, pd.DataFrame]):
    total = df['Time (ms)'].sum()
    avg = df['Time (ms)'].mean()
    min_time = df['Time (ms)'].min()
    max_time = df['Time (ms)'].max()
    print('Total time (ms): ', total)
    print('Average time (ms): ', avg)
    print('Min time (ms): ', min_time)
    print('Max time (ms): ', max_time)


def store_data(data: dict) -> None:
    conn = connection_db(db_file='database.db')
    query = 'DROP TABLE IF EXISTS countries'
    query_statement(conn=conn, query=query)
    query = """ CREATE TABLE IF NOT EXISTS countries (
                id INTEGER PRIMARY KEY,
                region TEXT NOT NULL,
                city_name TEXT,
                language TEXT,
                time REAL
            ); """
            
    query_statement(conn=conn, query=query)
    save_data(
        conn=conn, 
        data=data, 
        query='INSERT INTO countries (region, city_name, language, time) VALUES (?, ?, ?, ?)'
    )
    store_in_file(conn=conn, query='SELECT region, city_name, language, time FROM countries')


def store_in_file(conn, query: str):   
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()         
    except Exception as e:
        raise SystemExit(e)    

    data = []
    for i in result:
        data.append({
            'region': i[0],
            'cityName': i[1],
            'language': i[2],
            'time': i[3],
        })

    try: 
        with open('data.json', 'w') as outfile:
            json.dump(data, outfile, indent=4)
    except Exception as e:
        raise SystemExit(e)   


if __name__ == '__main__':
    response = getAPI('https://restcountries.com/v3.1/all')
    if not response:
        print('No data to process')
    else:
        data = buildData(content=response.json())
        df = pd.DataFrame(data)
        print(df)        
        operations(df=df) 
        store_data(data=data)          
        