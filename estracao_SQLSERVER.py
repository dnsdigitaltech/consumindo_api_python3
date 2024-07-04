import requests  # Biblioteca para fazer requisições HTTP
import pandas as pd  # Biblioteca para manipulação de dados
from dotenv import load_dotenv  # Biblioteca para carregar variáveis de ambiente de um arquivo .env
import os  # Biblioteca para interagir com o sistema operacional
import time  # Biblioteca para manipulações relacionadas ao tempo

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Define a URL da API e a chave de autenticação
url = "https://xfinanceapp.com/api/1.1/obj/transactions"  # URL da API
token = os.getenv("API_TOKEN")  # Obtém o token de autenticação das variáveis de ambiente
headers = {"Authorization": f"Bearer {token}"}  # Define o cabeçalho de autenticação

# Função para chamar a API e obter todos os dados das transações em páginas.
def chamar_api_myfinance(url):
    lista_dados_todas_paginas = []  # Lista para armazenar todos os dados das páginas
    cursor = 0  # Inicializa o cursor para paginação

    while True:  # Loop para fazer requisições paginadas até que todos os dados sejam obtidos
        response = requests.get(url, headers=headers, params={"cursor": cursor})  # Faz a requisição à API
        response_ajustado_json = response.json()  # Converte a resposta para JSON
        dados_response = response_ajustado_json.get("response", None)  # Obtém a parte "response" do JSON
        
        if dados_response is not None:  # Se a resposta contiver dados
            results = dados_response.get('results', [])  # Obtém a lista de resultados
            remaining = dados_response.get('remaining', 0)  # Obtém o número de resultados restantes
            lista_dados_todas_paginas.extend(results)  # Adiciona os resultados à lista

            if remaining <= 0:  # Se não houver mais resultados, encerra o loop
                break
        else:  # Se a resposta não contiver dados, encerra o loop
            break
        
        cursor += 100  # Incrementa o cursor para a próxima página
        time.sleep(1)  # Aguarda 1 segundo antes da próxima requisição

    return lista_dados_todas_paginas  # Retorna a lista com todos os dados

# Chama a função para obter todos os dados das transações
lista_dados_todas_paginas = chamar_api_myfinance(url)
df = pd.DataFrame(lista_dados_todas_paginas)  # Converte a lista de dados em um DataFrame do pandas


############## AQUI FAZEMOS O VINCULO COM O BANCO DE DADOS SQL SERVER ############################

# instalar as seguintes bibliotecas:
# pip install pyodbc
# pip install sqlalchemy

# Importe as bibliotecas necessárias
from sqlalchemy.engine import URL  # Classe para criar URLs de conexão para o SQLAlchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, DATETIME  # Módulos para interagir com bancos de dados usando SQLAlchemy
from sqlalchemy.exc import IntegrityError  # Exceção específica do SQLAlchemy para tratar erros de integridade


# Converta as colunas de data para o formato aceito pelo SQL Server
data_columns = ['Modified Date', 'Created Date', 'estimated_date', 'payment_date']
for col in data_columns:  # Loop para converter cada coluna de data
    df[col] = pd.to_datetime(df[col], errors='coerce')  # Converte a coluna para o tipo datetime e se der erro o coerce transforma o valor com erro em Nat que significa Not a Time.

# Classe para gerenciar a conexão com o banco de dados SQL Server
class ConnectionHandler:
    def __init__(self, host, user, password, db):
        self.host = host  # Define o host do banco de dados
        self.user = user  # Define o usuário do banco de dados
        self.password = password  # Define a senha do banco de dados
        self.db = db  # Define o nome do banco de dados
        
        driver = "ODBC Driver 17 for SQL Server"  # Define o driver ODBC para SQL Server
        # Cria a string de conexão usando os detalhes fornecidos
        connection_string = f'DRIVER={driver};SERVER={self.host};PORT=1433;DATABASE={self.db};UID={self.user};PWD={self.password};&autocommit=true'
        # Cria a URL de conexão para o SQLAlchemy
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        # Cria o engine do SQLAlchemy com a URL de conexão
        self.engine = create_engine(connection_url, use_setinputsizes=False, echo=False)
        
        # Estabelece a conexão com o banco de dados
        self.db_connection = self.engine.connect()
        
    def fetch_data(self, query):
        # Método para executar uma consulta SQL e retornar os dados como um DataFrame
        return pd.read_sql(query, con=self.db_connection)
    
    def insert_data(self, df, tablename):
        # Método para inserir dados de um DataFrame em uma tabela do banco de dados
        df.to_sql(tablename, if_exists='append', index=False, con=self.db_connection, dtype={
            'Modified Date': DATETIME,                # Coluna de data
            'Created Date': DATETIME,                 # Coluna de data
            'Created By': String(255),                # Coluna de string com comprimento máximo
            'estimated_date': DATETIME,               # Coluna de data
            'recipient_ref': String(255),             # Coluna de string com comprimento máximo
            'status': String(255),                    # Coluna de string com comprimento máximo
            'amount': DECIMAL(10, 2),                 # Coluna numérica com precisão decimal
            'year_ref': Integer,                      # Coluna inteira
            'payment_date': DATETIME,                 # Coluna de data
            'OS_type-transaction': String(255),       # Coluna de string com comprimento máximo
            'user_ref': String(255),                  # Coluna de string com comprimento máximo
            'cod_ref': String(255),                   # Coluna de string com comprimento máximo
            'month_ref': Integer,                     # Coluna inteira
            'OS_frequency-type': String(255),         # Coluna de string com comprimento máximo
            '_id': String(255)                        # Coluna de string com comprimento máximo e chave primária
        })

    
    def execute_query(self, query):
        # Método para executar uma consulta SQL diretamente
        self.db_connection.execute(query)
        
    def __del__(self):
        # Método destrutor para fechar a conexão com o banco de dados
        try:
            self.db_connection.close()
        except:
            pass
    
# Carrega as credenciais do banco de dados do arquivo .env
db_user = os.getenv("DB_USER_SQLSERVER")
db_password = os.getenv("DB_PASSWORD_SQLSERVER")
db_host = os.getenv("DB_HOST_SQLSERVER")
db_name = os.getenv("DB_NAME_SQLSERVER")

# Cria a conexão com o banco de dados
CH = ConnectionHandler(db_host, db_user, db_password, db_name)

metadata = MetaData()

# Cria a tabela com todas as colunas necessárias e define a chave primária
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

metadata.create_all(CH.engine)  # Cria a tabela no banco de dados

# Insere o DataFrame no banco de dados SQL Server
try:
    CH.insert_data(df, 'transactions')
    print(f"Total de registros inseridos com sucesso: {len(df)}")  # Imprime o total de registros inseridos
except IntegrityError as e:
    print(f"Erro ao inserir dados: {e} Erro ao inserir dados")  # Trata exceção de integridade e imprime o erro

# Fechando a conexão explicitamente
del CH  # Chama o destrutor da classe para fechar a conexão com o banco de dados