0 Docker Container Setup
This contains the 'docker-compose.yml' for Docker container setup and the 'debeziumSettings.json' for setting up Debezium.

1 Kafka Producer & Consumer
This includes two types of Kafka units: one is the Kafka Producer that reads data, and the other is the Kafka Consumer that writes data. The Kafka Consumer works in conjunction with the Debezium connector to link Kafka with the Postgres Database.

2 Kafka to Postgres
The 'debeziumConnector.json' is used to set up the Debezium Connector, preparing it for the connection between Kafka and the database. Once the Debezium Connector is established, we can use 'PostgresConnect.ipynb' to complete the connection.

3 kafka_stream.py
Once all the components have been set up, we can use 'kafka_stream.py' to initiate the real-time data streaming process.