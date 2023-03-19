import os
import pandas as pd

def merge_csv_files(root_dir, target_fields):
    # Lista de diretórios que contém o arquivo 'Faturamento.csv'
    directories = []
    
    for dirpath, _, filenames in os.walk(root_dir):
        if 'Faturamento.csv' in filenames:
            directories.append(dirpath)
    
    if len(directories) == 0:
        print('Não foram encontrados diretórios com o arquivo "Faturamento.csv"')
        return
    
    # Dicionário para armazenar os dados de cada arquivo CSV
    csv_data = {}
    
    # Itera pelos diretórios que contém o arquivo 'Faturamento.csv' e faz a união dos arquivos
    for directory in directories:
        csv_file_path = os.path.join(directory, 'Faturamento.csv')
        
        # Lê o arquivo CSV e seleciona apenas as colunas desejadas
        csv_data[directory] = pd.read_csv(csv_file_path, usecols=target_fields)
        
    # Faz a união dos dados dos arquivos CSV
    merged_data = pd.concat(csv_data.values(), ignore_index=True)
    
    # Salva o arquivo consolidado na raiz do diretório
    merged_file_path = os.path.join(root_dir, 'Faturamento_consolidado.csv')
    merged_data.to_csv(merged_file_path, index=False)
    
    print(f'Arquivo consolidado salvo em: {merged_file_path}')

# Exemplo de uso
root_dir = 'C:/Users/Vinicius/Desktop/Sementeiros'
target_fields = ['a', 'b', 'c','d'] # Campos que serão mantidos na união dos arquivos
merge_csv_files(root_dir, target_fields)
