from kafka import KafkaConsumer
import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

def consume_data_and_store_to_s3():
    consumer = KafkaConsumer('customer_data_topic',
                             bootstrap_servers='192.168.15.22:9092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    s3_client = boto3.client(
        's3',
        region_name=os.getenv('S3_REGION_NAME'), # VAR NAME IN YOUR .env file
        aws_access_key_id=os.getenv('S3_ACCESS_KEY'), # VAR NAME IN YOUR .env file
        aws_secret_access_key=os.getenv('S3_SECRET_KEY') # VAR NAME IN YOUR .env file
    )

    for message in consumer:
        customer_data = message.value
        print("Received data:", customer_data)

        gene_one = customer_data.get('Gene One', 'unknown_gene_one')
        gene_two = customer_data.get('Gene Two', 'unknown_gene_two')
        file_name = f"gene_data_{gene_one}_{gene_two}.json"
        
        s3_client.put_object(
            Bucket='data-save-bucket',
            Key=f"gene_data/{file_name}",
            Body=json.dumps(customer_data)
        )
        print(f"Stored {file_name} to S3.")

if __name__ == '__main__':
    consume_data_and_store_to_s3()
