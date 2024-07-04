import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Define a URL da API e a chave de autenticação
url = "https://xfinanceapp.com/api/1.1/obj/transactions"
token = os.getenv("API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

# Função para chamar a API e obter todos os dados das transações em páginas.
def chamar_api_myfinance(url):
    lista_dados_todas_paginas = []
    cursor = 0

    while True:
        response = requests.get(url, headers=headers, params={"cursor": cursor})
        response_ajustado_json = response.json()
        dados_response = response_ajustado_json.get("response", None)
        
        if dados_response is not None:
            results = dados_response.get('results', [])
            remaining = dados_response.get('remaining', 0)
            lista_dados_todas_paginas.extend(results)

            if remaining <= 0:
                break
        else:
            break
        
        cursor += 100
        time.sleep(1)

    return lista_dados_todas_paginas

lista_dados_todas_paginas = chamar_api_myfinance(url)
df = pd.DataFrame(lista_dados_todas_paginas)


############## AQUI FAZEMOS O VINCULO COM O BANCO DE DADOS MYSQL ############################

# instalar as seguintes bibliotecas:
# pip install pymysql
# pip install sqlalchemy

# Importe as bibliotecas necessárias
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, DATETIME
from sqlalchemy.exc import IntegrityError

# Converta as colunas de data para o formato aceito pelo MySQL
data_columns = ['Modified Date', 'Created Date', 'estimated_date', 'payment_date']
for col in data_columns:
    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
    

# Conexão com o banco de dados MySQL - PUXANDO OS DADOS DO ARQUIVO .env
db_user = os.getenv("DB_USER") # nome do usuário no mysql é o root
db_password = os.getenv("DB_PASSWORD") # Você define na instalação, eu defini admin
db_host = os.getenv("DB_HOST") # Onde está hospedado o banco de dados, no meu caso localhost
db_name = os.getenv("DB_NAME") # Nome do schema no mysql que você criou, no meu caso db_xfinance

# string de conexão com o banco de dados, passando as variáveis do .env
engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}')

# Instanciando o objeto MetaData para criar a tabela no banco de dados
metadata = MetaData()

# Crie a tabela com todas as colunas necessárias e defina a chave primária
transactions_table = Table('transactions', metadata,
    Column('Modified Date', DATETIME),
    Column('Created Date', DATETIME),
    Column('Created By', String(255)),
    Column('estimated_date', DATETIME),
    Column('recipient_ref', String(255)),
    Column('status', String(255)),
    Column('amount', DECIMAL(10, 2)),
    Column('year_ref', Integer),
    Column('payment_date', DATETIME),
    Column('OS_type-transaction', String(255)),
    Column('user_ref', String(255)),
    Column('cod_ref', String(255)),
    Column('month_ref', Integer),
    Column('OS_frequency-type', String(255)),
    Column('_id', String(255), primary_key=True)
)

metadata.create_all(engine)

# Insere o DataFrame no banco de dados MySQL
inseridos_com_sucesso = 0

# Itera sobre cada linha do DataFrame
for index, row in df.iterrows():
    try:
        row.to_frame().T.to_sql('transactions', con=engine, if_exists='append', index=False)  # Converte a linha em um DataFrame de uma única linha e insere no banco de dados
        inseridos_com_sucesso += 1  # Incrementa o contador de registros inseridos com sucesso
    except IntegrityError as e:
        if 'Duplicate entry' in str(e.orig):  # Verifica se o erro é de entrada duplicada
            print(f"Erro ao inserir dados (entrada duplicada): {e}")  # Imprime uma mensagem específica para erro de entrada duplicada
        else:
            print(f"Erro ao inserir dados: {e}")  # Imprime uma mensagem para outros tipos de erro de integridade

print(f"Total de registros inseridos com sucesso: {inseridos_com_sucesso}")  # Imprime o total de registros que foram inseridos com sucesso