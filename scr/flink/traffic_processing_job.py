"""
Austin Traffic Telemetry – PyFlink Processing Job

This job:
1. Consumes JSON telemetry from the Kafka topic.
2. Uses the original event timestamp for event-time processing.
3. Applies a 10-second watermark for out-of-order records.
4. Groups records into 10-minute tumbling windows.
5. Calculates traffic statistics for each sensor and direction.
6. Uses checkpointing for fault recovery.
7. Prints the aggregated results.
"""

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVER = "kafka:29092"
KAFKA_TOPIC = "traffic-telemetry"
KAFKA_CONSUMER_GROUP = "traffic-flink-consumer"

# This JAR must be available inside the Flink containers.
KAFKA_CONNECTOR_JAR = (
    "file:///opt/flink/lib/"
    "flink-sql-connector-kafka-3.3.0-1.20.jar"
)

CHECKPOINT_INTERVAL_MS = 10_000


# ---------------------------------------------------------
# Create the Flink environment
# ---------------------------------------------------------

def create_flink_environment():
    """
    Create the Flink streaming and table environments.
    """

    configuration = Configuration()

    # Make the Kafka SQL connector available to the job.
    configuration.set_string(
        "pipeline.jars",
        KAFKA_CONNECTOR_JAR
    )

    environment = StreamExecutionEnvironment.get_execution_environment(
        configuration
    )

    # Three parallel tasks, matching the three Kafka partitions
    # and the three TaskManager slots.
    environment.set_parallelism(3)

    # Create a checkpoint every 10 seconds.
    environment.enable_checkpointing(
        CHECKPOINT_INTERVAL_MS
    )

    table_environment = StreamTableEnvironment.create(
    stream_execution_environment=environment
)

    # Prevent inactive Kafka partitions from blocking watermark progress.
    table_environment.get_config().set(
        "table.exec.source.idle-timeout",
        "10 s"
    )

    return environment, table_environment


# ---------------------------------------------------------
# Kafka source table
# ---------------------------------------------------------

def create_kafka_source(table_environment):
    """
    Create a Flink table connected to the Kafka telemetry topic.
    """

    source_ddl = f"""
        CREATE TABLE traffic_source (
            sensor_id STRING,
            event_time STRING,
            vehicle_count INT,
            direction STRING,
            movement STRING,
            ingestion_time STRING,

            event_timestamp AS TO_TIMESTAMP(
                event_time,
                'yyyy-MM-dd''T''HH:mm:ss'
            ),

            WATERMARK FOR event_timestamp
                AS event_timestamp - INTERVAL '10' SECOND
        )
        WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers'
                = '{KAFKA_BOOTSTRAP_SERVER}',
            'properties.group.id'
                = '{KAFKA_CONSUMER_GROUP}',

            'scan.startup.mode' = 'earliest-offset',

            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """

    table_environment.execute_sql(source_ddl)

    print("Kafka source table created successfully.")


# ---------------------------------------------------------
# Print output table
# ---------------------------------------------------------

def create_print_sink(table_environment):
    """
    Create an output table that prints aggregation results.
    """

    sink_ddl = """
        CREATE TABLE traffic_results (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            sensor_id STRING,
            direction STRING,
            total_vehicle_count BIGINT,
            average_vehicle_count DOUBLE,
            minimum_vehicle_count INT,
            maximum_vehicle_count INT,
            message_count BIGINT
        )
        WITH (
            'connector' = 'print'
        )
    """

    table_environment.execute_sql(sink_ddl)

    print("Print sink table created successfully.")


# ---------------------------------------------------------
# Windowed traffic aggregation
# ---------------------------------------------------------

def run_windowed_aggregation(table_environment):
    """
    Apply a 10-minute tumbling event-time window and calculate
    traffic statistics for every sensor and direction.
    """

    aggregation_query = """
        INSERT INTO traffic_results
        SELECT
            window_start,
            window_end,
            sensor_id,
            direction,

            CAST(SUM(vehicle_count) AS BIGINT)
                AS total_vehicle_count,

            CAST(AVG(vehicle_count) AS DOUBLE)
                AS average_vehicle_count,

            MIN(vehicle_count)
                AS minimum_vehicle_count,

            MAX(vehicle_count)
                AS maximum_vehicle_count,

            COUNT(*)
                AS message_count

        FROM TABLE(
            TUMBLE(
                TABLE traffic_source,
                DESCRIPTOR(event_timestamp),
                INTERVAL '10' MINUTES
            )
        )

        WHERE
            sensor_id IS NOT NULL
            AND event_timestamp IS NOT NULL
            AND vehicle_count IS NOT NULL

        GROUP BY
            window_start,
            window_end,
            sensor_id,
            direction
    """

    print("\nStarting the Flink traffic-processing job...")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {KAFKA_CONSUMER_GROUP}")
    print("Watermark delay: 10 seconds")
    print("Window size: 10 minutes")
    print("Press Ctrl + C to stop the job.\n")

    table_result = table_environment.execute_sql(
        aggregation_query
    )

    # Keep the streaming job running.
    table_result.wait()


# ---------------------------------------------------------
# Main program
# ---------------------------------------------------------

def main():
    """
    Run the complete PyFlink traffic-processing job.
    """

    print("=" * 60)
    print("Austin Traffic Telemetry – PyFlink Processing Job")
    print("=" * 60)

    _, table_environment = create_flink_environment()

    create_kafka_source(table_environment)
    create_print_sink(table_environment)
    run_windowed_aggregation(table_environment)


if __name__ == "__main__":
    main()