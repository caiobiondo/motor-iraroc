from Calculo.Calcular_cluster import *
import pandas as pd
import json
import sqlite3

def puxa_estoque():
    connection = sqlite3.connect("Bases/Banco_dados_att.db")
    dict_estoque = []
    resposta = pd.read_sql(f"SELECT DISTINCT id_grupo, trim(upper(rtg_op)) AS rtg_op, cd_cnpj FROM base_operacoes WHERE rtg_op <> '' AND rtg_op <> 'NR' AND rtg_op <> 'NR'", connection)
    lista_estoque = resposta.values

    for i in range(len(lista_estoque)):
        dict_estoque.append({"id_grupo":lista_estoque[i][0], "rtg_op":lista_estoque[i][1], "cd_cnpj":lista_estoque[i][2]})

    estoque = {"estoque": dict_estoque}

    return estoque