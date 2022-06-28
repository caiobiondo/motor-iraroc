import pandas as pd
import sqlite3
import math
# Funcao que retorna os precos de cva dado o prazo e o valor do fpr

def get_cva(prazo_anos, fpr, connDB):

    if(prazo_anos == 0):
        return 0
    
    elif(prazo_anos > 20):
        prazo_ano_teto = 20*360
        filtro = (f'SELECT pc_cva FROM acrescimo_cva WHERE vl_pz_oper = {prazo_ano_teto} AND pc_fpr = {fpr}')
        return pd.read_sql(filtro,connDB).values[0][0]  
    
    else:
        prazo_ano_teto = prazo_anos*360
        if(prazo_ano_teto < 0):
            return 0
        filtro = (f'SELECT pc_cva FROM acrescimo_cva WHERE vl_pz_oper = {prazo_ano_teto} AND pc_fpr = {fpr}')
        return pd.read_sql(filtro,connDB).values[0][0]

