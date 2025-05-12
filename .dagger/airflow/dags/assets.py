from airflow.sdk import Asset

ftp = Asset("kafka://local/ftp_intel")
targets = Asset("kafka://local/targets_intel")
services = Asset("kafka://local/services_intel")
shodan = Asset("kafka://local/shodan_raw")