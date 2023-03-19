import psycopg2
import csv

# Defina as informações de conexão do banco de dados
db_host = "localhost"
db_port = "5432"
db_name = "nome-do-seu-banco-de-dados"
db_user = "seu-usuario-do-banco-de-dados"
db_password = "sua-senha-do-banco-de-dados"

# Defina o caminho do arquivo CSV que você deseja ler
caminho_arquivo_csv = "caminho/do/seu/arquivo.csv"

# Defina o nome da tabela onde você deseja gravar os dados do arquivo CSV
nome_tabela = "nome-da-sua-tabela"

# Conecte-se ao banco de dados usando a biblioteca psycopg2
try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    print("Conexão bem-sucedida ao banco de dados.")
except psycopg2.Error as e:
    print("Erro ao conectar ao banco de dados:", e)
    exit()

# Crie um cursor para executar comandos SQL
cur = conn.cursor()

# Leia o arquivo CSV e insira os dados no banco de dados
with open(caminho_arquivo_csv, 'r') as arquivo_csv:
    leitor_csv = csv.reader(arquivo_csv)
    cabecalho = next(leitor_csv)  # Ler o cabeçalho do arquivo CSV
    for linha in leitor_csv:
        valores = [int(linha[0]), linha[1], float(linha[2])]  # Defina os valores da linha como uma lista
        comando_sql = f"INSERT INTO {nome_tabela} ({cabecalho[0]}, {cabecalho[1]}, {cabecalho[2]}) VALUES (%s, %s, %s)"  # Crie o comando SQL para inserir os valores na tabela
        cur.execute(comando_sql, valores)  # Execute o comando SQL com os valores da linha

# Salve as alterações no banco de dados
conn.commit()

# Feche a conexão com o banco de dados
conn.close()
