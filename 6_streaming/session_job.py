from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_session_sink(t_env):
    table_name = 'taxi_trip_sessions'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pu_location_id INT,
            do_location_id INT,
            streak_length BIGINT,
            streak_start TIMESTAMP(3),
            streak_end TIMESTAMP(3),
            PRIMARY KEY (pu_location_id, do_location_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_watermark AS lpep_dropoff_datetime,
            WATERMARK for dropoff_watermark as dropoff_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_session():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[2]  # We treat the second tuple element as the event-time (ms).
        )
    )
    try:
        # Create Kafka table for green trips
        source_table = create_events_source_kafka(t_env)
        streaks_table = create_events_session_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {streaks_table}
        SELECT
            PULocationID as pu_location_id,
            DOLocationID as do_location_id,
            COUNT(*) AS streak_length,
            MIN(dropoff_watermark) AS streak_start,
            MAX(dropoff_watermark) AS streak_end
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(dropoff_watermark), INTERVAL '5' MINUTE)
        )
        GROUP BY PULocationID, DOLocationID, window_start, window_end
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_session()
