import requests
import pandas as pd

token = "Aqui fica o TOKEN"

headers = {"api_key": f"{token}"}

urlContas = "https://"

urlProdutos = "https://"

def callApi(url):
    response = requests.get(url,headers=headers)
    return response

responseContas = callApi(urlContas)

responseProdutos = callApi(urlProdutos)

jsonContas = responseContas.json()['dados']

jsonProdutos = responseProdutos.json()['dados']

dfContas = pd.DataFrame(jsonContas,columns=['id_conta','nome'])

dfProdutos = pd.DataFrame(jsonProdutos,columns=['id_produto','nome'])

dfContas.to_excel('Contas.xlsx',index=False)

dfProdutos.to_excel('Produtos.xlsx',index=False)

dfContas.to_parquet('Contas.parquet',index=False)

dfProdutos.to_parquet('Produtos.parquet',index=False)