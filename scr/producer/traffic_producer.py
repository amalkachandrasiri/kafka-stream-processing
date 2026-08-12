'''
Austin Traffic Telemetry Kafka Producer

This producer:
1. Reads historical Austin traffic-count records from a CSV file.
2. Converts each record into a structured JSON telemetry message.
3. Sends one message to the Kafka topic every two seconds.
4. Uses the sensor/device ID as the Kafka message key.
5. Simulates occasional out-of-order events for watermark testing.
'''

import json
import random
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = 'traffic-telemetry'

# data location 
CSV_FILE = Path('data/Camera_Traffic_Counts_20260811_trimmed.csv')

MESSAGE_INTERVAL_SECONDS = 2

# Set to None to continue until the whole CSV is processed.
# Stop the producer at any time using Ctrl + C.
MAX_RECORDS = None

# Enables occasional out-of-order messages.
SIMULATE_OUT_OF_ORDER = True

# Every 10 records, a small group will be shuffled.
SHUFFLE_BATCH_SIZE = 10

# ---------------------------------------------------------
# Column names expected in the Austin dataset
# ---------------------------------------------------------

SENSOR_ID_COLUMN = 'ATD Device ID'
TIMESTAMP_COLUMN = 'Read Date'
VEHICLE_COUNT_COLUMN = 'Volume'
DIRECTION_COLUMN = 'Direction'
MOVEMENT_COLUMN = 'Movement'

# ---------------------------------------------------------
# Helper functions
# ---------------------------------------------------------

def clean_value(value):
    '''
    Convert pandas and NumPy values into JSON-compatible values.
    Missing values are converted to None.
    '''

    if pd.isna(value):
        return None

    if hasattr(value, 'item'):
        return value.item()

    return value


def validate_columns(dataframe):
    '''
    Check whether all required columns exist in the CSV file.
    '''

    required_columns = [
        SENSOR_ID_COLUMN,
        TIMESTAMP_COLUMN,
        VEHICLE_COUNT_COLUMN,
        DIRECTION_COLUMN,
        MOVEMENT_COLUMN,
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            'The following required columns are missing from the CSV: '
            f"{missing_columns}\n\n"
            f"Available columns are:\n{dataframe.columns.tolist()}"
        )


def load_traffic_data(csv_path):
    '''
    Read and prepare the Austin traffic dataset.
    '''

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file was not found: {csv_path.resolve()}"
        )

    dataframe = pd.read_csv(csv_path)

    print(f"CSV file loaded: {csv_path}")
    print(f"Original number of records: {len(dataframe)}")

    validate_columns(dataframe)

    # Convert the event timestamp to a proper datetime value.
    dataframe[TIMESTAMP_COLUMN] = pd.to_datetime(
        dataframe[TIMESTAMP_COLUMN],
        errors='coerce'
    )

    # Convert vehicle count to numeric.
    dataframe[VEHICLE_COUNT_COLUMN] = pd.to_numeric(
        dataframe[VEHICLE_COUNT_COLUMN],
        errors='coerce'
    )

    # Remove records that cannot be processed.
    dataframe = dataframe.dropna(
        subset=[
            SENSOR_ID_COLUMN,
            TIMESTAMP_COLUMN,
            VEHICLE_COUNT_COLUMN,
        ]
    ).copy()

    # Vehicle counts should be whole numbers.
    dataframe[VEHICLE_COUNT_COLUMN] = (
        dataframe[VEHICLE_COUNT_COLUMN].astype(int)
    )

    # Arrange records by event time before streaming.
    dataframe = dataframe.sort_values(
        by=TIMESTAMP_COLUMN
    ).reset_index(drop=True)

    if MAX_RECORDS is not None:
        dataframe = dataframe.head(MAX_RECORDS)

    print(f"Valid records available for streaming: {len(dataframe)}")

    if not dataframe.empty:
        print(
            'Event-time range:',
            dataframe[TIMESTAMP_COLUMN].min(),
            'to',
            dataframe[TIMESTAMP_COLUMN].max()
        )

    return dataframe


def create_kafka_producer():
    '''
    Create and return a Kafka producer.
    '''

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],

        # Convert Python dictionaries into JSON bytes.
        value_serializer=lambda value: json.dumps(
            value,
            default=str
        ).encode('utf-8'),

        # Convert the sensor ID into bytes for use as the Kafka key.
        key_serializer=lambda key: str(key).encode('utf-8'),

        # Wait for Kafka to acknowledge the message.
        acks='all',

        retries=5,
    )

    print(
        f"Connected to Kafka broker: {KAFKA_BOOTSTRAP_SERVER}"
    )

    return producer


def create_telemetry_message(row):
    '''
    Convert one CSV row into a structured telemetry message.
    '''

    event_time = row[TIMESTAMP_COLUMN]

    telemetry_message = {
        'sensor_id'    : str(row[SENSOR_ID_COLUMN]),
        'event_time'   : event_time.isoformat(),
        'vehicle_count': int(row[VEHICLE_COUNT_COLUMN]),
        'direction'    : clean_value(row[DIRECTION_COLUMN]),
        'movement'     : clean_value(row[MOVEMENT_COLUMN]),

        # Time at which the producer sends the record.
        'ingestion_time': datetime.now().astimezone().isoformat(),
    }

    return telemetry_message


def prepare_stream_records(dataframe):
    '''
    Prepare records for streaming.

    Small batches are occasionally shuffled to simulate records arriving
    slightly out of timestamp order. This will help demonstrate Flink's
    bounded-out-of-orderness watermark strategy.
    '''

    records = [
        row
        for _, row in dataframe.iterrows()
    ]

    if not SIMULATE_OUT_OF_ORDER:
        return records

    prepared_records = []

    for start in range(0, len(records), SHUFFLE_BATCH_SIZE):
        batch = records[start:start + SHUFFLE_BATCH_SIZE]

        # Shuffle every second batch only. This keeps most of the stream
        # ordered while introducing occasional late/out-of-order events.
        batch_number = start // SHUFFLE_BATCH_SIZE

        if batch_number % 2 == 1:
            random.shuffle(batch)

        prepared_records.extend(batch)

    return prepared_records


def send_telemetry(producer, records):
    '''
    Send records to Kafka one at a time.
    '''

    messages_sent = 0

    print(f"\nStreaming to Kafka topic: {KAFKA_TOPIC}")
    print(
        f"Message interval: {MESSAGE_INTERVAL_SECONDS} seconds"
    )
    print('Press Ctrl + C to stop the producer.\n')

    try:
        for row in records:
            telemetry_message = create_telemetry_message(row)
            sensor_id = telemetry_message['sensor_id']

            future = producer.send(
                topic=KAFKA_TOPIC,
                key=sensor_id,
                value=telemetry_message,
            )

            # Wait for Kafka to confirm the message and obtain metadata.
            metadata = future.get(timeout=10)

            messages_sent += 1

            print(
                f"Sent        #{messages_sent} | "
                f"Sensor:     {sensor_id} | "
                f"Count:      {telemetry_message['vehicle_count']} | "
                f"Event time: {telemetry_message['event_time']} | "
                f"Partition:  {metadata.partition} | "
                f"Offset:     {metadata.offset}"
            )

            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print('\nProducer stopped manually by the user.')

    except KafkaError as error:
        print(f"\nKafka error: {error}")
        raise

    finally:
        producer.flush()
        producer.close()

        print(f"Total messages sent: {messages_sent}")
        print('Kafka producer connection closed.')


# ---------------------------------------------------------
# Main program
# ---------------------------------------------------------

def main():
    '''
    Run the complete traffic telemetry producer.
    '''

    print('=' * 60)
    print('Austin Traffic Telemetry Producer')
    print('=' * 60)

    traffic_data = load_traffic_data(CSV_FILE)

    if traffic_data.empty:
        print('No valid traffic records are available to stream.')
        return

    stream_records = prepare_stream_records(traffic_data)
    kafka_producer = create_kafka_producer()

    send_telemetry(
        producer=kafka_producer,
        records=stream_records,
    )


if __name__ == '__main__':
    main()