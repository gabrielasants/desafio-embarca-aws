import json
import boto3
import psycopg2
from datetime import datetime
import os

def lambda_handler(event, context):
    # Variáveis de ambiente para as configurações do banco de dados
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_port = os.environ['DB_PORT']
    
    # Cliente para interagir com o S3
    s3_client = boto3.client('s3')
    
    # Recuperar informações do evento passado pela Lambda 1
    try:
        bucket_name = event['bucket_name']
        file_key = event['file_key']
    except KeyError as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f"Missing required information: {str(e)}")
        }

    # Baixar o arquivo CSV do S3
    try:
        csv_file = '/tmp/accidents.csv'
        s3_client.download_file(bucket_name, file_key, csv_file)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error downloading file from S3: {str(e)}")
        }
    
    # Calcular o número de mortos por tipo de veículo
    vehicle_types = ['automovel', 'bicicleta', 'caminhao', 'moto', 'onibus']
    deaths_by_vehicle = {}

    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            # Ler o arquivo CSV
            headers = f.readline().strip().split(',')
            for line in f:
                data = line.strip().split(',')
                row = dict(zip(headers, data))
                
                # Obter o trecho da estrada
                road_name = row['trecho']
                
                # Filtrar dados e calcular mortes
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
    
    # Conectar ao banco de dados
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
    
    # Criar a tabela se não existir
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
    
    # Inserir os dados no banco de dados
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
        # Fechar a conexão com o banco de dados
        cursor.close()
        connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data successfully processed and stored.')
    }
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

logger.info("Connecting to database")
# Código de conexão e inserção
logger.info("Data insertion completed successfully")
