import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

#### Set up environment
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

#### Define source table (Kafka)
source_ddl = """
CREATE TABLE user_events (
    ip STRING,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    referrer STRING,
    host STRING,
    url STRING,
    geodata STRING,
    event_timestamp TIMESTAMP(3),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bootcamp-events-prod',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
)
"""
t_env.execute_sql(source_ddl)

#### Define PostgreSQL sinks
t_env.execute_sql("""
CREATE TABLE processed_events_aggregated_khang (
    event_hour TIMESTAMP(3),
    avg_events_per_session DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/mydb',
    'table-name' = 'processed_events_aggregated_khang',
    'username' = 'postgres',
    'password' = 'postgres'
)
""")

#### Define PostgreSQL Sink for each host
t_env.execute_sql("""
CREATE TABLE processed_events_aggregated_host_khang (
    event_hour TIMESTAMP(3),
    host STRING,
    avg_events_per_session DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/mydb',
    'table-name' = 'processed_events_aggregated_host_khang',
    'username' = 'postgres',
    'password' = 'postgres'
)
""")

# ========== Overall Average Events per Session ==========
# Step 1: Group raw user events by 5-minute tumbling window and IP to define unique sessions
sessions = (
    t_env.from_path("user_events")  # Read from the registered source table
    .window(Tumble.over(lit(5).minutes).on(col("event_timestamp")).alias("w"))  # Define 5-minute tumbling window
    .group_by(col("w"), col("ip"))  # Group by window and IP (each IP per window = 1 session)
    .select(
        col("w").start.alias("event_hour"),  # Extract window start time
        col("ip")  # Keep the IP to count later
    )
)

# Step 2: Aggregate to compute total sessions and total events per 5-minute window
overall_avg = (
    sessions
    .group_by(col("event_hour"))  # Group by time window to get total sessions
    .select(
        col("event_hour"),
        col("ip").count.alias("total_sessions"),  # Count of unique IPs = total sessions
        lit(1).count.alias("total_events")  # Each row from 'sessions' represents one session = one event
    )
    .select(
        col("event_hour"),
        # Compute average events per session = total_events / total_sessions
        (col("total_events") / col("total_sessions"))
        .cast(DataTypes.DOUBLE())
        .alias("avg_events_per_session")
    )
)

# Step 3: Insert the result into a sink table for storage/visualization
overall_avg.execute_insert("processed_events_aggregated_khang")


# ========== Host-level Average Events per Session ==========
# Step 1: Define sessions by grouping on 5-minute window, IP, and host
sessions_by_host = (
    t_env.from_path("user_events")  # Read from the registered Flink source table
    .window(Tumble.over(lit(5).minutes).on(col("event_timestamp")).alias("w"))  # 5-minute tumbling window
    .group_by(col("w"), col("ip"), col("host"))  # Group by window, IP (session), and host
    .select(
        col("w").start.alias("event_hour"),  # Window start time
        col("ip"),                           # IP to represent a unique session
        col("host")                          # Include host for later aggregation
    )
)

# Step 2: Aggregate to compute total sessions and total events per host in each 5-minute window
avg_by_host = (
    sessions_by_host
    .group_by(col("event_hour"), col("host"))  # Aggregate per time window and host
    .select(
        col("event_hour"),
        col("host"),
        col("ip").count.alias("total_sessions"),  # Count of unique IPs = sessions for this host
        lit(1).count.alias("total_events")        # One row = one session, so total_events = row count
    )
    .select(
        col("event_hour"),
        col("host"),
        # Average events per session = total_events / total_sessions
        (col("total_events") / col("total_sessions"))
        .cast(DataTypes.DOUBLE())
        .alias("avg_events_per_session")
    )
)

# Step 3: Write the result to the sink table (PostgreSQL, etc.)
avg_by_host.execute_insert("processed_events_aggregated_host_khang")