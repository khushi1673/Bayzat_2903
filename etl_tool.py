import json
import os
import boto3
import psycopg2
from datetime import datetime, timedelta

# Configuration
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT", "http://localhost:4566")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "test-queue")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "sqs_data")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

def transform_message(msg_body):
    try:
        data = json.loads(msg_body)
    except json.JSONDecodeError:
        return None  # Handle malformed

    res = {
        "id": data.get("id"),
        "mail": data.get("mail"),
        "name": f"{data.get('name', '')} {data.get('surname', '')}".strip(),
        "trip": {}
    }

    # Handle Route-based messages
    if "route" in data and data["route"]:
        route = data["route"]
        first = route[0]
        last = route[-1]
        
        res["trip"] = {
            "depaure": first.get("from"),
            "destination": last.get("to"),
            "start_date": first.get("started_at"),
            "end_date": last.get("started_at") # Simplified for now
        }
    
    # Handle Location-based messages
    elif "locations" in data and data["locations"]:
        locs = data["locations"]
        first = locs[0]
        last = locs[-1]
        
        def fmt_ts(ts):
            if isinstance(ts, int):
                return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            return ts

        res["trip"] = {
            "depaure": first.get("location"),
            "destination": last.get("location"),
            "start_date": fmt_ts(first.get("timestamp")),
            "end_date": fmt_ts(last.get("timestamp"))
        }
    
    return res

def init_db():
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            mail TEXT,
            name TEXT,
            departure TEXT,
            destination TEXT,
            start_date TEXT,
            end_date TEXT
        )
    """)
    conn.commit()
    return conn

def main():
    # SQS Setup
    sqs = boto3.resource('sqs', endpoint_url=SQS_ENDPOINT, region_name=REGION, 
                         aws_access_key_id='test', aws_secret_access_key='test')
    
    try:
        queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
        # Set message retention to 7 days (604800 seconds)
        queue.set_attributes(Attributes={
            'MessageRetentionPeriod': '604800'
        })
        print(f"Queue {SQS_QUEUE_NAME} configured with 7-day retention period.")
    except Exception as e:
        print(f"Error finding queue: {e}")
        return

    # DB Setup
    print("Initializing Database...")
    db_conn = init_db()
    db_cur = db_conn.cursor()

    print(f"Processing messages from {SQS_QUEUE_NAME}...")
    while True:
        messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5)
        if not messages:
            print("No more messages. Exiting.")
            break
        
        for msg in messages:
            print(f"Received message {msg.message_id}")
            transformed = transform_message(msg.body)
            
            if transformed:
                # Persist to DB
                trip = transformed["trip"]
                db_cur.execute("""
                    INSERT INTO trips (user_id, mail, name, departure, destination, start_date, end_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    transformed["id"], transformed["mail"], transformed["name"],
                    trip.get("depaure"), trip.get("destination"), 
                    trip.get("start_date"), trip.get("end_date")
                ))
            
            # Delete from queue
            msg.delete()
        
        db_conn.commit()

    db_conn.close()
    print("ETL complete.")

if __name__ == "__main__":
    main()
