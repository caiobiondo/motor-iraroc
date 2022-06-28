import pandas as pd
import numpy as np
import sqlite3 as sql
import logging

def get_rating_cnpj(id):
    conn = sql.connect('Bases/Banco_dados_att.db')

    id_numero = int(id)
    filtro = (f'SELECT rtg_ajustado FROM base_operacoes WHERE id_grupo = {id_numero} ')
    rating = pd.read_sql(filtro, conn).values[0][0]  
    return rating

def get_lista_rating(segmento):
    if(segmento == "PRIVATE_PF"):
        lista_private_pf = ["AA3", "BAA1", "B2", "C3"]
        return lista_private_pf
    elif(segmento == "PRIVATE_PJ"):
        lista_private_pj = ["BAA3", "BA5", "B2", "B4"]
        return lista_private_pj
    else:
        lista_resto = ["AAA", "AA3", "A1", "A3", "A4", "BAA1", "BAA3", "BAA4", "BA1", "BA4", "BA5", "BA6", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "D1", "D3", "E1", "F1", "G1", "H"]
        return lista_resto
def get_rating_middle_cnpj(cnpj):
    conn = sql.connect('Bases/rating_middle.db')

    filtro = (f'SELECT rating FROM rating_middle WHERE cnpj = "{cnpj}" ')
    rating = pd.read_sql(filtro, conn).values[0][0]  
    return rating 