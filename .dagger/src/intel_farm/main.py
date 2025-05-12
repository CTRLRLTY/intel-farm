import dagger
from datetime import datetime
from dagger import dag, function, object_type


@object_type
class IntelFarm:
    @function
    async def load_image(self, docker: dagger.Socket) -> str:
        postgres_ball = (await self.postgres()).as_tarball()
        server_ball = (await self.apiserver()).as_tarball()
        kafka_broker_ball = (await self.kafka_broker()).as_tarball()
        kafka_controller_ball = (await self.kafka_controller()).as_tarball()
        airflow_ball = (await self.airflow()).as_tarball()

        fileball = [("postgres.tar", postgres_ball), 
                    ("api_server.tar", server_ball), 
                    ("kafka_broker.tar", kafka_broker_ball), 
                    ("kafka_controller.tar", kafka_controller_ball), 
                    ("airflow.tar", airflow_ball)]

        docker_cli = (
            dag.container()
            .from_("docker:cli")
            .with_env_variable("INVALIDATE_CACHE_NOW", str(datetime.now()))
            .with_unix_socket("/var/run/docker.sock", docker)
        )

        for file_name, tarball in fileball:
            docker_cli = await docker_cli.with_mounted_file(file_name, tarball)
            out = await docker_cli.with_exec(f"docker load -i {file_name}".split()).stdout()
            image = out.strip().split(":", 1)[1].strip()
            tag = file_name.split(".")[0]
            await docker_cli.with_exec(f"docker tag {image} local/{tag}:latest".split()).stdout()
            
        return "done"

    @function
    async def postgres(self) -> dagger.Container:
        pgdb = (
            dag.container()
            .from_("postgres:latest")
            .with_exposed_port(5432)
            .with_env_variable("POSTGRES_DB", "shodan")
            .with_env_variable("POSTGRES_PASSWORD", "password")
        )
        return await pgdb
    
    @function
    async def apiserver(self) -> dagger.Container:
        server_dir = await dag.current_module().source().directory("/server")
        server = (
            dag.container()
            .from_("python:3.12-bookworm")
            .with_workdir("/server")
            .with_exposed_port(8000)
            .with_directory("/server", server_dir)
            .with_mounted_cache("/root/.cache/pip", dag.cache_volume("apiserver-pip"))
            .with_exec(["pip", "install", "-r", "requirements.txt"])
            # .with_service_binding("postgres", db)
            .with_entrypoint(args=["fastapi", "run", "server.py", "--port", "8000"])
        )

        return await server

    @function
    async def kafka_broker(self) -> dagger.Container:
        kafka = (
            dag.container()
            .from_('apache/kafka:4.0.0')
            .with_exposed_port(9092)
            .with_env_variable("KAFKA_NODE_ID", "2")

            .with_env_variable("KAFKA_PROCESS_ROLES", "broker")
            .with_env_variable("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .with_env_variable("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")

            .with_env_variable("KAFKA_LISTENERS", "PLAINTEXT://:9092")
            .with_env_variable("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka_broker:9092")

            .with_env_variable("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka_controller:9093")
            .with_env_variable("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")

            .with_env_variable("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", 
                               "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")

            # must be set to 1 if only using 1 broker
            .with_env_variable("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .with_env_variable("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env_variable("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")

            .with_env_variable("KAFKA_NUM_PARTITIONS", "1") # just 1 for no reason
            # .as_service()
            # .with_hostname("kafka")
        )

        return await kafka
    
    @function
    async def kafka_controller(self) -> dagger.Container:
        kafka = (
            dag.container()
            .from_('apache/kafka:4.0.0')
            .with_exposed_port(9093)
            .with_env_variable("KAFKA_NODE_ID", "1")

            .with_env_variable("KAFKA_PROCESS_ROLES", "controller")
            .with_env_variable("KAFKA_LISTENERS", "CONTROLLER://:9093")

            .with_env_variable("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .with_env_variable("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")

            .with_env_variable("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka_controller:9093")
            .with_env_variable("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        )

        return await kafka
    
    @function
    async def airflow(self) -> dagger.Container:
        airflow_dir = await dag.current_module().source().directory("airflow")
        con = (
            dag.container()
            .from_("python:3.12-bookworm")
            .with_exposed_port(8080)
            .with_directory("/airflow", airflow_dir)
            .with_workdir("/airflow")
            .with_mounted_cache("/root/.cache/pip", dag.cache_volume("airflow-pip"))
            .with_env_variable("AIRFLOW_HOME", "/airflow")
            .with_new_file("simple_auth_manager_passwords.json.generated", '{"admin": "password"}')
            .with_exec(["pip", "install", 
                        'apache-airflow[postgres]==3.0.0', 
                        'apache-airflow-providers-apache-kafka==1.8.1',
                        'shodan',
                        "--constraint", 
                        "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.9.txt"])
            .with_exec("airflow db migrate".split())
            .with_exec("airflow connections import conn.json".split())
            # .with_service_binding("postgres", db)
            # .with_service_binding("kafka", kafka)
            .with_entrypoint(args=["airflow", "standalone"])
        )

        return await con

    @function
    async def test_api_server(self) -> str:
        postgres: dagger.Container = (await self.postgres())
        server: dagger.Container = (await self.apiserver())
        ftp_sql = await dag.current_module().source().file("airflow/dags/sql/ftp.sql")
        services_sql = await dag.current_module().source().file("airflow/dags/sql/services.sql")
        targets_sql = await dag.current_module().source().file("airflow/dags/sql/targets.sql")
        test_py = await dag.current_module().source().file("test/test_rest_api.py")

        postgres_service = (
            postgres
            .with_mounted_file("/docker-entrypoint-initdb.d/02-targets.sql", targets_sql)
            .with_mounted_file("/docker-entrypoint-initdb.d/03-ftp.sql", ftp_sql)
            .with_mounted_file("/docker-entrypoint-initdb.d/04-services.sql", services_sql)
            .with_new_file("/docker-entrypoint-initdb.d/05-init.sql", """
                INSERT INTO targets (ip_addr, org)
                VALUES
                    ('192.0.0.1', 'org1'),
                    ('192.0.0.2', 'org2');
                           
                INSERT INTO ftp (target, is_anon)
                VALUES
                    ('192.0.0.1', true),
                    ('192.0.0.2', true);
            """)
            .as_service()
        )

        server_service = (            
            server
            .with_service_binding("postgres", postgres_service)
            .as_service()
        )

        out = (
            dag.container()
            .from_("python:3.12")
            .with_service_binding("api-server", server_service)
            .with_env_variable("ENV_HTTP_ENDPOINT", "http://api-server:8000")
            .with_workdir("/test")
            .with_mounted_file("/test/test_rest_api.py", test_py)
            .with_mounted_cache("/root/.cache/pip", dag.cache_volume("test-api-server-pip"))
            .with_exec("pip install requests".split())
            .with_exec("python test_rest_api.py".split())
        )

        return await out.stdout()