import pendulum
from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import assets
import shodan
import json

SHODAN_API_KEY = ""
shodan_api = shodan.Shodan(SHODAN_API_KEY)
kafka_conn = "kafka_shodan"
kafka_ftp_conn = "kafka_shodan_ftp"
kafka_svc_conn = "kafka_shodan_services"
kafka_tgt_conn = "kafka_shodan_targets"

def produce_ftp_batch(messages):
	kk_hook = KafkaProducerHook(kafka_config_id=kafka_conn)
	kk_producer = kk_hook.get_producer()

	for message in messages:
		data = json.loads(message.value())

		if "ftp" in data:
			val = {}
			val['ip'] = data['ip_str']
			val['is_anon'] = data['ftp']['anonymous']
			timestamp = data['timestamp']

			kk_producer.produce(topic="ftp_intel", key=json.dumps(timestamp), value=json.dumps(val))
		
	kk_producer.flush()


def produce_service_batch(messages):
	kk_hook = KafkaProducerHook(kafka_config_id=kafka_conn)
	kk_producer = kk_hook.get_producer()
	
	for message in messages:
		data = json.loads(message.value())
		
		if "ftp" in data:
			val = {}
			val['ip'] = data['ip_str'] 
			val['port'] = data['port']
			val['name'] = "ftp"
			val['status'] = "open"
			timestamp = data['timestamp']
			kk_producer.produce(topic="services_intel", key=json.dumps(timestamp), value=json.dumps(val))

	kk_producer.flush()


def produce_target_batch(messages):
	kk_hook = KafkaProducerHook(kafka_config_id=kafka_conn)
	kk_producer = kk_hook.get_producer()

	for msg in messages:
		data = json.loads(msg.value())
		val = {}
		val['ip'] = data['ip_str'] 
		val['org'] = data['org']
		timestamp = data['timestamp']

		kk_producer.produce(topic="targets_intel", key=json.dumps(timestamp), value=json.dumps(val))
	kk_producer.flush()


def fetch_ftp():
	# results = shodan_api.search("port:21")
	with open("/airflow/dags/misc/shodan.json") as f:
		results = json.load(f)

	for res in results["matches"]:
		yield (None, json.dumps(res))


@dag(
	schedule=None,
	start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
	catchup=False,
	tags=["intel"],
	is_paused_upon_creation=False
)
def shodan_flow_etl():
	query_shodan_ftp = ProduceToTopicOperator(
		kafka_config_id=kafka_conn,
		task_id="query_shodan_ftp",
		topic="shodan_ftp_raw",
		producer_function=fetch_ftp,
		outlets=[assets.shodan]
	)

	produce_ftp_task = ConsumeFromTopicOperator(
		kafka_config_id=kafka_ftp_conn,
		task_id="produce_ftp",
		topics=["shodan_ftp_raw"],
		apply_function_batch=produce_ftp_batch,
		commit_cadence="end_of_batch",
		max_messages=100,
		max_batch_size=10,
		poll_timeout=10
	)

	produce_services_task = ConsumeFromTopicOperator(
		kafka_config_id=kafka_svc_conn,
		task_id="produce_service",
		topics=["shodan_ftp_raw"],
		apply_function_batch=produce_service_batch,
		commit_cadence="end_of_batch",
		max_messages=100,
		max_batch_size=10,
		poll_timeout=10
	)

	produce_targets_task = ConsumeFromTopicOperator(
		kafka_config_id=kafka_tgt_conn,
		task_id="produce_target",
		topics=["shodan_ftp_raw"],
		apply_function_batch=produce_target_batch,
		commit_cadence="end_of_batch",
		outlets=[assets.targets],
		max_messages=100,
		max_batch_size=10,
		poll_timeout=10
	)

	query_shodan_ftp >> [produce_targets_task, produce_ftp_task, produce_services_task]
	
shodan_flow_etl()