import boto3

def lambda_handler(event, context):
    client = boto3.client('glue')
    client.start_crawler(Name='retail-raw-crawler')
