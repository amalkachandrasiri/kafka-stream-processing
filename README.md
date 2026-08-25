Austin Traffic Telemetry: Kafka and PyFlink Stream Processing

This project implements a real-time traffic-data pipeline using Apache Kafka and Apache Flink. Historical Austin camera traffic-count records are replayed as a live stream, published to Kafka as JSON messages, and processed through a stateful PyFlink job using event-time watermarks and 10-minute tumbling windows.

The implementation covers:

A single-node Kafka broker operating in KRaft mode.

A traffic-telemetry topic with three partitions.

A Python producer that publishes one record every two seconds.

A PyFlink pipeline with a 10-second bounded-out-of-orderness watermark.

Ten-minute tumbling-window vehicle-count aggregation by sensor and direction.

Flink checkpointing for state recovery.

Monitoring through the Apache Flink Dashboard.

Architecture

flowchart LR
    CSV[Austin traffic CSV] --> Producer[Python producer]
    Producer --> Kafka[Kafka: traffic-telemetry]
    Kafka --> Source[PyFlink source]
    Source --> Window[Watermark + 10-minute window]
    Window --> Sink[Aggregated Print sink]

The producer runs on the Windows host and connects to Kafka through localhost:9092. The PyFlink job runs inside Docker and therefore connects to Kafka through the internal Docker address kafka:29092.

Technology Stack

Docker and Docker Compose

Apache Kafka in KRaft mode

Apache Flink 1.20.3

Python 3.10

Apache PyFlink 1.20.3

kafka-python

Flink Kafka SQL connector 3.3.0-1.20

Project Structure

kafka-stream-processing/
|-- data/
|   `-- Camera_Traffic_Counts_20260811_trimmed.csv
|-- lib/
|   `-- flink-sql-connector-kafka-3.3.0-1.20.jar
|-- scr/
|   |-- flink/
|   |   `-- traffic_processing_job.py
|   `-- producer/
|       `-- traffic_producer.py
|-- .env
|-- docker-compose.yml
|-- Dockerfile.flink
|-- requirements.txt
`-- README.md

The project uses the directory name scr, not src. The volume mapping in docker-compose.yml must therefore use ./scr/flink:/opt/flink/jobs.

Data Format

The producer converts each valid CSV row into a JSON message with the following structure:

{
  "sensor_id": "6,957",
  "event_time": "2022-10-05T10:15:00",
  "vehicle_count": 23,
  "direction": "NORTHBOUND",
  "movement": "THRU",
  "ingestion_time": "2026-08-12T14:45:30"
}

The sensor ID is used as the Kafka message key. This keeps records belonging to the same sensor in the same partition and preserves their order within that partition.

Prerequisites

Install the following before running the project:

Docker Desktop

Python 3.10 or a compatible Python 3 version

PowerShell

A Python virtual environment containing the packages in requirements.txt

Docker Desktop must be running before the Compose environment is started.

Configuration

The Compose file reads its container names, ports and other settings from .env. Ensure that the file is available in the project root before starting the services.

Important network addresses are:

Component

Address

Used by

Kafka external listener

localhost:9092

Python producer on Windows

Kafka internal listener

kafka:29092

PyFlink inside Docker

Flink Dashboard

http://localhost:8081

Web browser

The producer configuration in scr/producer/traffic_producer.py should use:

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

The Flink job in scr/flink/traffic_processing_job.py should use:

KAFKA_BOOTSTRAP_SERVER = "kafka:29092"

Running the Project

Run all commands from the project root.

1. Activate the Python environment

.\venv\Scripts\Activate.ps1

Install the local Python dependencies if required:

pip install -r requirements.txt

2. Validate the Compose configuration

docker compose config

The command should print the resolved configuration without YAML or environment-variable errors.

3. Build and start Kafka and Flink

docker compose up -d --build

Confirm that the services are running:

docker compose ps

The expected containers are:

traffic-kafka

traffic-flink-jobmanager

traffic-flink-taskmanager

4. Create the Kafka topic

Automatic topic creation is disabled. Create the topic explicitly:

docker exec traffic-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic traffic-telemetry --partitions 3 --replication-factor 1

Verify the topic configuration:

docker exec traffic-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic traffic-telemetry

The output should show:

PartitionCount: 3
ReplicationFactor: 1

A replication factor of one is used because the development environment contains one Kafka broker. Each partition therefore has one copy and no broker-level redundancy.

5. Verify the PyFlink environment

Confirm the Python and PyFlink installations inside the JobManager:

docker exec traffic-flink-jobmanager python --version

docker exec traffic-flink-jobmanager python -c "from importlib.metadata import version; print(version('apache-flink'))"

Confirm that the processing script is mounted correctly:

docker exec traffic-flink-jobmanager ls -l /opt/flink/jobs

The output should include traffic_processing_job.py.

6. Submit the PyFlink job

Check whether a job is already running:

docker exec traffic-flink-jobmanager flink list

If no traffic-processing job is running, submit it:

docker exec -it traffic-flink-jobmanager flink run -py /opt/flink/jobs/traffic_processing_job.py

The terminal should confirm:

Kafka source table creation.

Print sink table creation.

Kafka topic traffic-telemetry.

Consumer group traffic-flink-consumer.

Watermark delay of 10 seconds.

Window size of 10 minutes.

A submitted Flink Job ID.

Open the Flink Dashboard at http://localhost:8081. The job should appear under Jobs > Running Jobs.

7. Run the traffic producer

Open another PowerShell terminal, activate the virtual environment and run:

python .\scr\producer\traffic_producer.py

The producer reads the Austin traffic CSV and sends one JSON message every two seconds. Its terminal output displays the sensor ID, vehicle count, event time, assigned Kafka partition and offset.

Allow enough messages to pass through several event-time windows before stopping the producer with Ctrl + C. Stopping the producer does not stop Kafka or Flink.

8. Optionally verify the raw Kafka messages

Run a Kafka console consumer in another terminal:

docker exec -it traffic-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic traffic-telemetry --from-beginning

Press Ctrl + C after confirming that structured JSON messages are displayed.

PyFlink Processing Logic

Event-time processing

The original traffic timestamp is converted into a Flink timestamp and used as event time. This means each record is assigned to a window according to when the traffic observation occurred, rather than when Flink received it.

Bounded out-of-orderness watermark

The source table uses a 10-second watermark delay:

WATERMARK FOR event_timestamp
    AS event_timestamp - INTERVAL '10' SECOND

This allows slightly out-of-sequence records to be reconciled before Flink considers the relevant event-time interval complete.

Idle-partition handling

The source idle timeout prevents inactive Kafka partitions from blocking the combined watermark:

table_environment.get_config().set(
    "table.exec.source.idle-timeout",
    "10 s"
)

This is important when the test messages use the same sensor key and are therefore routed to only one Kafka partition.

Ten-minute tumbling windows

The records are grouped by sensor ID and direction using non-overlapping 10-minute windows. For each group, the pipeline calculates:

Total vehicle count

Average vehicle count

Minimum vehicle count

Maximum vehicle count

Number of processed messages

Although the task description refers to a moving total, a tumbling window produces a separate aggregate for each non-overlapping 10-minute interval.

Checkpointing

Checkpointing is enabled every 10 seconds:

environment.enable_checkpointing(10_000)

The checkpoint state supports recovery if a processing task fails or restarts.

Verifying the Results

Confirm that the Flink job is running

docker exec traffic-flink-jobmanager flink list

The job status should be RUNNING.

Display the aggregated results

docker logs traffic-flink-taskmanager --since 15m 2>&1 | Select-String -SimpleMatch "+I["

An example result is:

+I[2022-10-05T10:10, 2022-10-05T10:20, 6,957, WESTBOUND, 319, 79.0, 1, 299, 4]

The fields represent:

window start, window end, sensor ID, direction,
total, average, minimum, maximum, message count

In this example, sensor 6,957 produced a westbound total of 319 vehicles during the 10:10-10:20 window, based on four source messages.

Inspect the execution graph

In the Flink Dashboard:

Open Jobs > Running Jobs.

Select the traffic-processing job.

Open Overview.

The graph should display:

Kafka source, calculation and local window aggregation.

Global window aggregation and Print sink.

Parallelism of three.

All operators in the RUNNING state.

Inspect checkpoints

Open the Checkpoints tab for the running job. Confirm that checkpoints are being triggered and completed and that all parallel operator tasks acknowledge the latest completed checkpoint.

Stopping or Restarting the Application

List the active Flink job and copy its Job ID:

docker exec traffic-flink-jobmanager flink list

Cancel the job when required:

docker exec traffic-flink-jobmanager flink cancel <JOB_ID>

Stop the Docker services without deleting their persistent volumes:

docker compose down

Start them again with:

docker compose up -d

After restarting the containers, submit the PyFlink job again if it is not listed as running.

Troubleshooting

NoBrokersAvailable from the Python producer

The host-based producer must use:

localhost:9092

It must not use the Docker-only address kafka:29092.

Also confirm that Kafka is healthy:

docker compose ps

UnknownTopicOrPartitionException

The traffic-telemetry topic does not exist. Create it using the topic-creation command provided above and confirm all three partitions using the describe command.

The Flink job remains in RESTARTING

Check the JobManager errors:

docker logs traffic-flink-jobmanager --since 30m 2>&1 | Select-String -Pattern "ERROR|Exception|Caused by|Failed" -Context 3,12

Also check the Exceptions tab in the Flink Dashboard.

No aggregation output appears

Confirm that:

The Flink job is RUNNING.

The producer has sent records covering more than one event-time window.

The source idle timeout is configured.

The TaskManager log command uses Select-String -SimpleMatch "+I[".

The -SimpleMatch option is important because + has a special meaning in regular expressions.

/opt/flink/jobs is empty

Confirm that the Compose volume mapping uses the actual project folder name:

- ./scr/flink:/opt/flink/jobs

Recreate the JobManager after correcting the mapping:

docker compose up -d --force-recreate jobmanager

Large TaskManager log output

Avoid following the complete log unless necessary. Use a filtered command:

docker logs traffic-flink-taskmanager --since 15m 2>&1 | Select-String -SimpleMatch "+I["

If docker logs -f is used, press Ctrl + C to stop viewing the log. This does not stop the Flink service.
