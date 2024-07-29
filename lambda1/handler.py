import boto3
import requests
import yaml
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
def get_available_bucket(bucket_prefix):
    try:
        response = s3.list_buckets()
        buckets = response.get('Buckets', [])
        
        for bucket in buckets:
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
        
        raise ValueError("Nenhum bucket disponível com o prefixo fornecido.")
    
    except ClientError as e:
        raise RuntimeError(f"Erro ao listar buckets: {str(e)}")

def download_csv(event, context):
    try:
        with open("link.yml", "r") as file:
            data = yaml.safe_load(file)
        
        if data is None or 'link' not in data:
            raise ValueError("Arquivo YAML está vazio ou a chave 'link' está faltando.")
        
        link = data['link']
        response = requests.get(link)
        response.raise_for_status()
        file_name = link.split('/')[-1]
        bucket_name = get_available_bucket('desafio-embarca')
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=response.content)
        result = {
            'statusCode': 200,
            'body': {
                'file_name': file_name,
                'bucket_name': bucket_name
            }
        }
        print("Download e upload concluídos com sucesso:", result)
        return result

    except ValueError as e:
        error_result = {
            'statusCode': 400,
            'body': f'Erro no arquivo YAML: {str(e)}'
        }
        print("Erro no arquivo YAML:", error_result)
        return error_result

    except requests.exceptions.RequestException as e:
        error_result = {
            'statusCode': 500,
            'body': f'Erro ao baixar o arquivo: {str(e)}'
        }
        print("Erro ao baixar o arquivo:", error_result)
        return error_result

    except ClientError as e:
        error_result = {
            'statusCode': 500,
            'body': f'Erro ao salvar no S3: {str(e)}'
        }
        print("Erro ao salvar no S3:", error_result)
        return error_result

    except yaml.YAMLError as e:
        error_result = {
            'statusCode': 500,
            'body': f'Erro ao carregar o arquivo YAML: {str(e)}'
        }
        print("Erro ao carregar o arquivo YAML:", error_result)
        return error_result

    except RuntimeError as e:
        error_result = {
            'statusCode': 500,
            'body': f'Erro ao obter bucket disponível: {str(e)}'
        }
        print("Erro ao obter bucket disponível:", error_result)
        return error_result

# Simulação para testes locais
class Context:
    def __init__(self):
        self.function_name = "download_csv"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:download_csv"
        self.aws_request_id = "12345678-1234-1234-1234-123456789012"

event = {} 
context = Context()

# Executar a função Lambda localmente
response = download_csv(event, context)
print("Resposta final:", response)
