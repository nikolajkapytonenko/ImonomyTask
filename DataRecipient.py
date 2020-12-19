import requests
from bs4 import BeautifulSoup
import sqlite3
import random
import time


def get_date_from():
    day_duration = 86400
    date_from_sec = time.time() - 7*day_duration
    date_from_ts = time.localtime(date_from_sec)
    date_from = time.strftime("%Y-%m-%d", date_from_ts)
    return date_from


def get_date_to():
    return time.strftime("%Y-%m-%d")


def get_data(date_from=get_date_from(), date_to=get_date_to()):

    parameters = {"key": "4e923a83-8b31-4c28-ac3d-f6cb994a60af", "type_name": "dsp", "format": "xml",
                  "date_from": date_from, "date_to": date_to}
    url = url = "http://publishers.imonomy.com/api/reports"
    api_xml = requests.get(url, params=parameters)
    api_data = api_xml.text
    soup = BeautifulSoup(api_data, 'xml')
    all_rows = soup.find_all("row")
    report = list()

    if all_rows:
        for row in all_rows:
            record = list()
            date = row.find("date").text.strip()
            if date:
                record.append(date)
            else:
                break

            client_name = "name" + str(random.randint(100, 10000))
            record.append(client_name)

            provider_name = row.find("provider_name").text.strip()
            if provider_name:
                record.append(provider_name)
            else:
                break

            revenue = row.find("revenue").text.strip()
            if revenue:
                record.append(revenue)
            else:
                break

            wons = row.find("wons").text.strip()
            if wons:
                record.append(wons)
            else:
                break

            report.append(record)
    return report


def add_data_to_db():

    report = get_data()

    db_name = "Data.db"
    table_name = "API_Data"

    query_api_data_table = """
        CREATE TABLE IF NOT EXISTS {0}
        ({1} TEXT NOT NULL,
        {2} TEXT NOT NULL,
        {3} TEXT NOT NULL,
        {4} INTEGER NOT NULL,
        {5} REAL NOT NULL);
        """.format(table_name, "Date", "Client_name", "Provider_name", "Wons", "Revenue")

    query_add_data = "INSERT INTO {0} VALUES (?, ?, ?, ?, ?);".format(table_name)
    db_conn = sqlite3.connect(db_name)
    db_cur = db_conn.cursor()

    try:
        db_cur.execute(query_api_data_table)
        db_cur.executemany(query_add_data, report)
    except sqlite3.DatabaseError as db_err:
        print(db_err)
    else:
        db_conn.commit()
    finally:
        db_cur.close()
        db_conn.close()


def get_data_from_db():
    db_name = "Data.db"
    table_name = "API_Data"
    query = "SELECT * FROM {0} ORDER BY Date".format(table_name)

    db_conn = sqlite3.connect(db_name)
    db_cur = db_conn.cursor()
    table = list()
    try:
        data = db_cur.execute(query)

        for line in data:
            table.append(line)
    except sqlite3.DatabaseError as db_err:
        print(db_err)
    else:
        db_conn.commit()
    finally:
        db_cur.close()
        db_conn.close()
        return table
