#!/usr/bin/env python
# coding: utf-8

# In[8]:


from confluent_kafka import Producer
import requests
import json
import time

# Define the Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Adjust the Kafka broker address as needed
    'client.id': 'randomuser_producer'
}

# Define the Kafka topic
kafka_topic = 'user_profiles'

# Create a Kafka producer instance
producer = Producer(producer_config)

# Function to fetch user profiles from the API and publish to Kafka
def fetch_and_publish_user_profiles():
    user_count = 0
    while True:  # Continue fetching until you choose to stop
        try:
            response = requests.get(api_url, timeout=5)  # Add a timeout here (in seconds)

            if response.status_code == 200:
                data = response.json()['results'][0]
                message_value = json.dumps(data).encode('utf-8')
                producer.produce(topic=kafka_topic, value=message_value)

                producer.flush()

                user_count += 1
                print(f"Published user profile to Kafka topic: {kafka_topic}")
        except Exception as e:
            # Handle request exceptions (e.g., network errors)
            print(f"Request to API failed: {e}")
            time.sleep(2)  # Add a delay before retrying

        # Add a 2-second delay between fetching users
        time.sleep(2)

# Define the API URL for randomuser.me
api_url = "https://randomuser.me/api/?results=1"  # You can adjust the number of results as needed

# Run the function to continuously fetch and publish user profiles
fetch_and_publish_user_profiles()

