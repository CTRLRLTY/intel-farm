import pendulum
from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import assets
import json


@dag(
	schedule=assets.targets,
	start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
	catchup=False,
	tags=["intel"],
	is_paused_upon_creation=False
)
def store_targets_pgdb():
	db_conn = "pg_upsert_target"

	def upsert_target(messages):
		stmt = """INSERT INTO targets (ip_addr, org) 
					VALUES (%s, %s)
					ON CONFLICT(ip_addr) DO UPDATE
					SET org = EXCLUDED.org
					WHERE targets.org IS DISTINCT FROM EXCLUDED.org;"""
		rows = []

		for msg in messages:
			row = json.loads(msg.value())
			rows.append((row["ip"], row["org"]))

		pg_hook = PostgresHook(postgres_conn_id=db_conn)
		pg_conn = pg_hook.get_conn()
		pg_cursor = pg_conn.cursor()
		pg_cursor.execute("BEGIN")
		pg_cursor.executemany(stmt, rows)
		pg_conn.commit()

	create_table = SQLExecuteQueryOperator(
		task_id="create_targets_table",
		conn_id=db_conn,
		sql="sql/targets.sql"
	)

	consume_intel = ConsumeFromTopicOperator(
		kafka_config_id="kafka_upsert_tgt_pg",
		task_id="upsert_target_intel",
		topics=["targets_intel"],
		apply_function_batch=upsert_target,
		outlets=[assets.ftp, assets.services],
		poll_timeout=10
	)

	create_table >> consume_intel


@dag(
	schedule=assets.ftp,
	start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
	catchup=False,
	tags=["intel"],
	is_paused_upon_creation=False
)
def store_ftp_pgdb():
	db_conn = "pg_upsert_ftp"

	def upsert_ftp(messages):
		stmt = """INSERT INTO ftp (target, is_anon) 
					VALUES (%s, %s) 
					ON CONFLICT(target) DO UPDATE
					SET is_anon = EXCLUDED.is_anon
					WHERE ftp.is_anon IS DISTINCT FROM EXCLUDED.is_anon"""
		rows = []

		for msg in messages:
			row = json.loads(msg.value())
			rows.append((row["ip"], row["is_anon"]))

		pg_hook = PostgresHook(postgres_conn_id=db_conn)
		pg_conn = pg_hook.get_conn()
		pg_cursor = pg_conn.cursor()
		pg_cursor.execute("BEGIN")
		pg_cursor.executemany(stmt, rows)
		pg_conn.commit()

	create_table = SQLExecuteQueryOperator(
		task_id="create_ftp_table",
		conn_id=db_conn,
		sql="sql/ftp.sql"
	)

	consume_intel = ConsumeFromTopicOperator(
		kafka_config_id="kafka_upsert_ftp_pg",
		task_id="upsert_ftp_intel",
		topics=["ftp_intel"],
		apply_function_batch=upsert_ftp,
		poll_timeout=10
	)

	create_table >> consume_intel


@dag(
	schedule=assets.services,
	start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
	catchup=False,
	tags=["intel"],
	is_paused_upon_creation=False
)
def store_services_pgdb():
	db_conn = "pg_upsert_service"

	create_table = SQLExecuteQueryOperator(
		task_id="create_services_table",
		conn_id=db_conn,
		sql="sql/services.sql"
	)

	def upsert_service(messages):
		stmt = """INSERT INTO services (target, name, status, port)
					VALUES (%s, %s, %s, %s)
					ON CONFLICT(target, port) DO UPDATE
					SET status = EXCLUDED.status, name = EXCLUDED.name
					WHERE services.status IS DISTINCT FROM EXCLUDED.status"""
		rows = []

		for msg in messages:
			row = json.loads(msg.value())
			rows.append((row["ip"], row["name"], row['status'], row['port']))

		pg_hook = PostgresHook(postgres_conn_id=db_conn)
		pg_conn = pg_hook.get_conn()
		pg_cursor = pg_conn.cursor()
		pg_cursor.execute("BEGIN")
		pg_cursor.executemany(stmt, rows)
		pg_conn.commit()

	consume_intel = ConsumeFromTopicOperator(
		kafka_config_id="kafka_upsert_svc_pg",
		task_id="upsert_service_intel",
		topics=["services_intel"],
		apply_function_batch=upsert_service,
		poll_timeout=10
	)

	create_table >> consume_intel

store_targets_pgdb()
store_services_pgdb()
store_ftp_pgdb()
