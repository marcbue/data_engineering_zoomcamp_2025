from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_session_sink(t_env):
    table_name = 'processed_events_session'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pu_location_id INT,
            do_location_id INT,
            streak_length BIGINT,
            streak_start TIMESTAMP(3),
            streak_end TIMESTAMP(3)
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
            WATERMARK FOR dropoff_watermark AS dropoff_watermark - INTERVAL '5' SECOND
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

    try:
        # Create Kafka table for green trips
        source_table = create_events_source_kafka(t_env)
        session_table = create_events_session_sink(t_env)

        t_env.execute_sql(f"""
            INSERT INTO {session_table}
            SELECT
                PULocationID AS pu_location_id,
                DOLocationID AS do_location_id,
                COUNT(*) AS streak_length,
                MIN(lpep_dropoff_datetime) AS streak_start,
                MAX(lpep_dropoff_datetime) AS streak_end
            FROM {source_table}
            GROUP BY
                SESSION(dropoff_watermark, INTERVAL '5' MINUTES),
                PULocationID,
                DOLocationID
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_session()
