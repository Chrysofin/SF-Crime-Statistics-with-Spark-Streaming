import producer_server

#/usr/bin/zookeeper-server-start ./config/zookeeper.properties
#/usr/bin/kafka-server-start ./config/server.properties
#/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic JSON_TOPICS --from-beginning
#/usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test

def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.udacity.project.sfcrimepolice",
        bootstrap_servers="localhost:9093",
        client_id="1"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
