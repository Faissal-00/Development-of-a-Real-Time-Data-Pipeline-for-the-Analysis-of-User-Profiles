# Development-of-a-Real-Time-Data-Pipeline-for-the-Analysis-of-User-Profiles

## I. Environment

To set up the required environment for this project, follow these steps:

### 1. Install Docker

If you don't already have Docker installed, download and install it from the official Docker website: [Get Docker](https://www.docker.com/get-started).

### 2. Pull Docker Images

Run this yml to pull the necessary Docker images for Kafka, Cassandra, and MongoDB with this code :
 - Code:
    ```bash
    docker-compose -f docker-compose.yml up -d

- docker-compose.yml :
  ```bash
    version: '3'

    services:
      zookeeper:
        image: wurstmeister/zookeeper
        container_name: zookeeper
        ports:
          - "2181:2181"
    
      kafka:
        image: wurstmeister/kafka
        container_name: kafka
        ports:
          - "9092:9092"
        environment:
          KAFKA_ADVERTISED_HOST_NAME: localhost
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    
      cassandra:
        image: cassandra
        container_name: cassandra
        ports:
          - "9042:9042"
    
      mongo:
        image: mongo
        container_name: mongo
        ports:
          - "27017:27017"
   

 - Installing and Configuring Spark
   ``` bash
    sudo apt install default-jdk
    wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
    tar xvf spark-3.5.0-bin-hadoop3.tgz
    sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
    nano ~/.bashrc
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin
    source ~/.bashrc
    spark-shell


## II. Start Kafka  

### 1. Access the Kafka Container
- Code:
  ```bash
  docker exec -it kafka /bin/sh  

### 2. Create a Kafka Topic

- Code:
  ```bash
  kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic user_profiles  

### 3. Verify the Topic Creation

- Code:
  ```bash
  kafka-topics.sh --list --zookeeper zookeeper:2181

### 4. Use the Kafka Console Producer

- Code:
  ```bash
  python3 Producer.py
  
### 5. Use the Kafka Console Consumer

- Code:
  ```bash
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 Consumer.py


## III. Start Cassandra

### 1.Access the Cassandra Container
- Code :
  ```bash
  docker exec -it cassandra cqlsh

### 2. Create the keyspace "user_profiles" in Cassandra
- Code :
  ```bash
  keyspace = "user_profiles"
  session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
  session.execute(f"USE {keyspace}")

### 3. List all keyspaces   
- Code :
  ```bash
    DESCRIBE KEYSPACES;

### 4. Switch to your keyspace and Create a Table
- Code :
  ```bash
  table_name = "users"
  table_creation_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        full_name TEXT PRIMARY KEY,
        calculated_age INT,
        complete_address TEXT
    )
  """
  session.execute(table_creation_query)

### 5. Describe the table and see its schema
- Code :
  ```bash
    DESCRIBE TABLE Users;
