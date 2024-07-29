import json
import boto3
import psycopg2
from datetime import datetime
import os

def get_secret():
    secret_name = os.environ['DB_SECRET_ARN']
    client = boto3.client('secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except Exception as e:
        raise ValueError(f"Error retrieving secret: {str(e)}")

def get_available_bucket(bucket_prefix):
    s3 = boto3.client('s3')
    try:
        response = s3.list_buckets()
        buckets = response.get('Buckets', [])
        
        for bucket in buckets:
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
        
        raise ValueError("Nenhum bucket disponível com o prefixo fornecido.")
    
    except boto3.exceptions.Boto3Error as e:
        raise RuntimeError(f"Erro ao listar buckets: {str(e)}")

def lambda_handler(event, context):
    secret = get_secret()
    
    db_host = secret['host']
    db_name = secret['dbname']
    db_user = secret['username']
    db_password = secret['password']
    db_port = secret['port']
    
    s3_client = boto3.client('s3')
    
    try:
        bucket_prefix = 'desafio-embarca'
        bucket_name = get_available_bucket(bucket_prefix)
        file_key = event['file_key']
    except KeyError as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f"Missing required information: {str(e)}")
        }
    except ValueError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error getting bucket: {str(e)}")
        }
    try:
        csv_file = '/tmp/accidents.csv'
        s3_client.download_file(bucket_name, file_key, csv_file)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error downloading file from S3: {str(e)}")
        }
    
    vehicle_types = ['automovel', 'bicicleta', 'caminhao', 'moto', 'onibus']
    deaths_by_vehicle = {}

    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            headers = f.readline().strip().split(',')
            for line in f:
                data = line.strip().split(',')
                row = dict(zip(headers, data))
                road_name = row['trecho']
                for vehicle in vehicle_types:
                    if int(row[vehicle]) > 0:
                        if vehicle not in deaths_by_vehicle:
                            deaths_by_vehicle[vehicle] = {}
                        if road_name not in deaths_by_vehicle[vehicle]:
                            deaths_by_vehicle[vehicle][road_name] = 0
                        deaths_by_vehicle[vehicle][road_name] += int(row['mortos'])
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing CSV file: {str(e)}")
        }
    try:
        connection = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        cursor = connection.cursor()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error connecting to database: {str(e)}")
        }
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS accident_stats (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP,
            road_name VARCHAR(255),
            vehicle VARCHAR(50),
            number_deaths INT
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error creating table: {str(e)}")
        }
    try:
        insert_query = """
        INSERT INTO accident_stats (created_at, road_name, vehicle, number_deaths)
        VALUES (%s, %s, %s, %s)
        """
        for vehicle, roads in deaths_by_vehicle.items():
            for road_name, deaths in roads.items():
                cursor.execute(insert_query, (datetime.now(), road_name, vehicle, deaths))
        connection.commit()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error inserting data into database: {str(e)}")
        }
    finally:
        cursor.close()
        connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data successfully processed and stored.')
    }
