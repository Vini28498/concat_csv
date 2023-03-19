import boto3
import pandas as pd

# configuração do cliente S3
s3 = boto3.resource('s3')
client = boto3.client('s3')

# diretórios de origem e destino
bucket_origem = 'nome-do-bucket-de-origem'
bucket_destino = 'nome-do-bucket-de-destino'

# nome do diretório a ser percorrido
diretorio = 'nome-do-diretorio'

# campos que serão mantidos nos arquivos consolidados
campos_necessarios = ['campo1', 'campo2', 'campo3']

# lista para armazenar os dados dos arquivos .csv
dataframes = []

# função para buscar arquivos .csv em um diretório e seus subdiretórios
def buscar_arquivos(diretorio):
    # percorre o diretório e seus subdiretórios
    for obj in s3.Bucket(bucket_origem).objects.filter(Prefix=diretorio):
        # verifica se é um arquivo .csv
        if obj.key.endswith('.csv'):
            # lê o arquivo e adiciona ao dataframe
            df = pd.read_csv(f's3://{bucket_origem}/{obj.key}')
            dataframes.append(df[campos_necessarios])

# chama a função para buscar os arquivos .csv
buscar_arquivos(diretorio)

# concatena os dataframes
df_concatenado = pd.concat(dataframes)

# grava o arquivo consolidado no S3
df_concatenado.to_csv(f's3://{bucket_destino}/{diretorio}.csv', index=False)

print('Arquivo consolidado gravado com sucesso!')
