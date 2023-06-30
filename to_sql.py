import requests
import json
import pandas as pd
import schedule
from sqlalchemy import create_engine


def get_data(url):
    response = requests.get(url=url)
    data = response.json()
    return data

def format_data(data):

    rename_data = {}

    for k, v in data.items():

        rename_data[k] =   {
            "Cambio": v['code'],
            "Moeda": v['codein'],
            "NomedoCambio": v['name'],
            "Máximo": v['high'],
            "Minimo": v['low'],
            "Variação": v['varBid'],
            "pctVariação": v['pctChange'],
            "Compra": v['bid'],
            "Venda": v['ask'],
            "timestamp": "1687960718",
            "Data/Hora": v['create_date']
        }

    rename_data = pd.DataFrame.from_dict(rename_data, orient='index')
    return rename_data

def salvar_sql(data):
    df = pd.DataFrame(data)
    
    db_conn = 'mysql+pymysql://root:root@localhost:3306/cambio_data'
    db_conn = create_engine(db_conn)
    df.to_sql(con=db_conn, name='cambio_brl', if_exists='replace', index=False)

def main():
    url = ('https://economia.awesomeapi.com.br/json/last/BRL-USD,BRL-PLN,BRL-ARS,BRL-CHF,BRL-CLP,BRL-JPY,BRL-MXN,BRL-AED,BRL-CNY,BRL-CZK,BRL-GBP,BRL-VEF')

    get_dados = get_data(url)
    data = format_data(get_dados)

    salvar_sql(data)

    print('\033[94m\033[1mDados Atualizados na Database\033[0m')

schedule.every(30).seconds.do(main)

while True:
    schedule.run_pending()