import requests
import json
from datetime import datetime
from csv import writer
from urllib.parse import urljoin
import sqlite3
from sqlite3 import Error

class ConfigurationHelper:
    # Placeholder values
    Username = "user@example.com"
    Password = "password123"
    SerialNumber = "12345678"

def fetch_token(email, password):
    url = "https://nep.nepviewer.com/pv_monitor/appservice/login"
    data = {'email': email, 'password': password}
    response = requests.post(url, data=data)
    if response.status_code == 200:
        try:
            response_data = response.json()  # Assuming the response should be JSON
            # Check if the expected data is in the expected format
            if isinstance(response_data, dict):
                # Navigate through the dictionary as expected
                token_data = response_data.get('data', {})
                if isinstance(token_data, dict):
                    return token_data.get('Token')
            elif isinstance(response_data, list) and response_data:  # In case the response is a list
                # Process this case if it's known how the list is structured
                print("Response JSON is a list, not expected dict. Check the API and data received.")
        except ValueError:
            print("Failed to parse JSON response")
    return None

def login(username, password):
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.110 Mobile Safari/537.36',
        'Accept': 'application/json'
    })
    url = "https://user.nepviewer.com/pv_manager/check.php"
    data = {'username': username, 'password': password}
    response = session.post(url, data=data)
    if response.status_code == 200 and 'PHPSESSID' in session.cookies:
        print("Logged in with Session ID:", session.cookies['PHPSESSID'])
        return True
    return False

def get_daily_power_consumption(sn):
    url = f"https://nep.nepviewer.com/pv_monitor/appservice/detail/{sn}"
    response = requests.get(url)
    if response.status_code == 200:
        data = json.loads(response.text.replace("null", "0"))
        max_watts_by_time = {}
        for item in data:
            if item:
                date_time = datetime.fromtimestamp(item[0] / 1000)
                date_time = date_time.replace(microsecond=0)
                watt = int(item[1])
                if date_time not in max_watts_by_time or watt > max_watts_by_time[date_time]:
                    max_watts_by_time[date_time] = watt
        return [(k, v) for k, v in max_watts_by_time.items()]  # Convert dictionary to list of tuples
    return []

def remove_unwanted_duplicates(metrics):
    if len(metrics) == 0:
        return metrics

    # Group by time and remove the zero watt if there's another with the same time and watt > 0
    grouped_by_time = {}
    for metric in metrics:
        time = metric[0]  
        watts = metric[1]
        if time not in grouped_by_time:
            grouped_by_time[time] = []
        grouped_by_time[time].append(metric)

    # Check the last group since it's sorted by time
    last_group_time = sorted(grouped_by_time.keys())[-1]
    last_group = grouped_by_time[last_group_time]

    if len(last_group) > 1 and any(watt == 0 for time, watt in last_group) and any(watt > 0 for time, watt in last_group):
        # Remove the zero-watt metric if there's another with the same timestamp and a positive watt value
        metrics[:] = [metric for metric in metrics if not (metric[0] == last_group_time and metric[1] == 0)]
    
    return metrics  # Return the modified list



def create_connection(db_file):
    """ create a database connection to the SQLite database specified by db_file """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def create_table(conn):
    """ create a table from the create_table_sql statement """
    try:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT NOT NULL,
                watt INTEGER NOT NULL
            );
        """)
    except Error as e:
        print(e)

def insert_metric(conn, metric):
    """
    Insert a new metric into the metrics table
    :param conn:
    :param metric: (time, watt)
    :return: metric id
    """
    sql = ''' INSERT INTO metrics(time, watt)
              SELECT ?, ?
              WHERE NOT EXISTS(SELECT 1 FROM metrics WHERE time = ? AND watt = ?); '''
    cur = conn.cursor()
    cur.execute(sql, metric + metric)
    conn.commit()
    return cur.lastrowid

def save_metrics_to_db(db_file, data):
    """
    Save list of metric data to SQLite database
    :param db_file: Database file path
    :param data: list of tuples (time, watt)
    """
    # Create a database connection
    conn = create_connection(db_file)
    if conn is not None:
        create_table(conn)  # ensure the table exists
        for metric in data:
            metric_id = insert_metric(conn, metric)
            if metric_id:
                print(f"Metric inserted with id: {metric_id}")
            else:
                print("Metric already exists and was not inserted.")
        conn.close()
    else:
        print("Error! cannot create the database connection.")

def main():
    print("Starting application...")
    email = ConfigurationHelper.Username
    password = ConfigurationHelper.Password
    sn = ConfigurationHelper.SerialNumber

    token = fetch_token(email, password)
    if token:
        print("Token fetched:", token)

    if login(email, password):
        data = get_daily_power_consumption(sn)
        cleaned_metrics = remove_unwanted_duplicates(data)
        save_metrics_to_db('metrics.db', cleaned_metrics)
        print("Data fetched:", cleaned_metrics)
    else:
        print("Failed to log in.")

if __name__ == "__main__":
    main()
