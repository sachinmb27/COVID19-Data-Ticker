import os
import sys
from dotenv import load_dotenv
import logging
from requests import Session
from time import sleep
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.futures import Future

# take environment variables from .env.
load_dotenv()

# environment variables
API_KEY = os.getenv('RAPID_API_KEY')
API_HOST = os.getenv('RAPID_API_HOST')
API_URL = os.getenv('API_URL')
GOOGLE_CREDENTIALS = os.getenv('JSON_CREDENTIAL_FILE')
PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC_ID = os.getenv('TOPIC_ID')

# setting Google service account key to the environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/app/keys/" + GOOGLE_CREDENTIALS

class PublishToPubSub:
    def __init__(self):
        self.project_id = PROJECT_ID
        self.topic_id = TOPIC_ID
        self.publisher_client = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []
    
    def get_covid_data(self) -> str:
        """Get real time COVID-19 data every hour"""

        # headers required for API call
        headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }

        # start session 
        session = Session()
        res = session.get(API_URL, headers=headers, stream=True)

        # response
        if 200 <= res.status_code < 400:
            logging.info(f"Response - {res.status_code}: {res.text}")
            return res.text
        else:
            logging.error(f"Failed to fetch API data - {res.status_code}: {res.text}")
            raise Exception(f"Failed to fetch API data - {res.status_code}: {res.text}")

    def get_callback(self, publish_future: Future, data: str) -> callable:
        """Callback function to handle publish failures"""

        def callback(publish_future):
            try:
                # wait 60 seconds for the publish call to succeed
                logging.info(publish_future.result(timeout=60))
                print(publish_future)
            except futures.TimeoutError:
                print(data)
                logging.error(f"Publishing {data} timed out.")
        
        return callback
    
    def publish_message_to_topic(self, message: str) -> None:
        """Publish message to a PubSub topic with an error handler"""

        # client returns a future when a message is published
        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))

        # publish failures are handled by the callback function
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)

        # waits for all the publish futures to resolve before exiting
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)

        logging.info(f"Published messages with error handler to {self.topic_path}.")

if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    svc = PublishToPubSub()

    # 24 hours in a day
    for i in range(24):
        message = svc.get_covid_data()
        svc.publish_message_to_topic(message)

        # collects data every 1 hour (3600 seconds)
        sleep(3600)