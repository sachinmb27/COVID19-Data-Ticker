import os
import logging
from base64 import b64decode
from dotenv import load_dotenv
from pandas import DataFrame
from json import loads
from google.cloud.storage import Client

# take environment variables from .env.
load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')

class LoadToStorage:
  def __init__(self, event, context):
    self.event = event
    self.context = context
    self.bucket_name = BUCKET_NAME
  
  def get_message(self) -> str:
    """Gets message from the PubSub topic"""

    logging.info(
      f"This message was triggered by messageId {self.context.event_id} published at {self.context.timestamp}"
      f" to {self.context.resource['name']}"
    )

    if "data" in self.event:
      topic_message = b64decode(self.event['data']).decode("utf-8")
      logging.info(f"Pubsub Message - {topic_message}")
      return topic_message
    else:
      logging.error(f"Error in message - Incorrect format")
      return ""
    
  def transform_json_to_dataframe(self, message: str) -> DataFrame:
    """Transforms data to support CSV format"""

    try:
      df = DataFrame(eval(message))
      if not df.empty:
        logging.info(f"Created DataFrame with {df.shape[0]} rows x {df.shape[1]} columns")
        return df
      else:
        logging.warning(f"Created empty DataFrame")
        return DataFrame()
    except Exception as e:
      logging.error(f"Encountered error creating DataFrame - {str(e)}")
      raise
  
  def upload_to_bucket(self, df: DataFrame, file_name: str = "payload") -> None:
    """Uploads data stored in CSV files to the storage bucket"""

    storage_client = Client()
    bucket = storage_client.bucket(self.bucket_name)
    blob = bucket.blob(f"{file_name}.csv")

    blob.upload_from_string(data=df.to_csv(index=False), content_type="text/csv")

    logging.info(f"File {file_name}.csv uploaded to {self.bucket_name}")
  
def loading_process(event, context):
  """Main function"""

  root = logging.getLogger()
  root.setLevel(logging.INFO)

  svc = LoadToStorage(event, context)

  message = svc.get_message()
  upload_df = svc.transform_json_to_dataframe(message)
  payload_timestamp = upload_df['Last Update'].unique()[0]

  svc.upload_to_bucket(upload_df, "covid_data_" + str(payload_timestamp))