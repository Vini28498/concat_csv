import boto3
import pandas as pd
import io

s3 = boto3.resource('s3')
bucket_name = 'seu-bucket'

def merge_csv_files(bucket_name, prefix):
    bucket = s3.Bucket(bucket_name)
    files = []
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('.csv'):
            files.append(obj.key)
    
    if len(files) == 0:
        print(f'Não foram encontrados arquivos .csv no prefixo {prefix}')
        return
    
    # Faz a união dos arquivos csv com o mesmo nome
    merged_data = {}
    for file in files:
        key_name = file.split('/')[-1]
        if key_name not in merged_data:
            merged_data[key_name] = pd.read_csv(io.BytesIO(s3.Object(bucket_name, file).get()['Body'].read()))
        else:
            merged_data[key_name] = pd.concat([merged_data[key_name], pd.read_csv(io.BytesIO(s3.Object(bucket_name, file).get()['Body'].read()))])
    
    # Grava os arquivos consolidados no novo bucket
    for key in merged_data.keys():
        merged_csv_data = merged_data[key]
        new_file_name = f'{prefix}/{key}'
        csv_buffer = io.StringIO()
        merged_csv_data.to_csv(csv_buffer, index=False)
        s3.Object(bucket_name, new_file_name).put(Body=csv_buffer.getvalue())

# Exemplo de uso
merge_csv_files(bucket_name, 'diretorio-pai')
