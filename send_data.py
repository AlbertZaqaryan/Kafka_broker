from kafka import KafkaProducer
import csv
import json

def produce_csv_data(file_path):
    producer = KafkaProducer(bootstrap_servers='192.168.15.22:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send('customer_data_topic', row)

if __name__ == '__main__':
    produce_csv_data('gene_expression.csv')
