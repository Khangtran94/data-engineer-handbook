from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment

def create_processed_events_sink_kafka(t_env):
    #### Create Sink for Kafka
    table_name = "process_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    sasl_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.jaas.config' = '{sasl_config}',
            'format' = 'json'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name

def create_processed_events_sink_postgres(t_env):
    ### Create sink for PostgreSQL
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

class GetLocation(ScalarFunction):
    ### UDF to fetch geolocation data via API
    def eval(self, ip_address):
        try:
            response = requests.get(
                "https://api.ip2location.io",
                params={
                    'ip': ip_address,
                    'key': os.environ.get("IP_CODING_KEY")
                },
                timeout=2  # avoid hanging
            )
            if response.status_code == 200:
                data = response.json()
                return json.dumps({
                    'country': data.get('country_code', ''),
                    'state': data.get('region_name', ''),
                    'city': data.get('city_name', '')
                })
        except Exception as e:
            # Log the error or count failures
            return json.dumps({})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())

def create_events_source_kafka(t_env):
    ### Event source table in Kafka
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    print('Starting Job!')
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.create_temporary_function("get_location", get_location)
    try:
        #### Create Kafka table
        source_table = create_events_source_kafka(t_env)
        #### Create PostgreSQL sink
        postgres_sink = create_processed_events_sink_postgres(t_env)
        print('loading into postgres')
        #### Load data from Kafka table to PostgreSQL sink
        #### note that use 5 minute gap => INTERVAL '5' MINUTE
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                    ip,
                    SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS session_start,
                    SESSION_END(event_timestamp, INTERVAL '5' MINUTE) AS session_end,
                    referrer,
                    host,
                    url,
                    get_location(ip) as geodata
                FROM {source_table}
                GROUP BY
                    SESSION(event_timestamp, INTERVAL '5' MINUTE),
                    ip,
                    host,
                    referrer,
                    url
                """
                    ).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_processing()