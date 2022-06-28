import math
import sqlite3 as sql
import pandas as pd

def rcp_a_b(n_pz_dc, n_alfa, n_beta):
    
    n_pz_dc_be = 0 
    n_rcp = 0
    n_k = 2 

    if (n_beta != 0):
        n_pz_dc_be = n_k * math.exp(-n_alfa / n_beta);
    else:
        n_pz_dc_be = 0
    

    if (n_pz_dc > n_pz_dc_be):
        n_rcp = n_alfa + n_beta * math.log(n_pz_dc)
    else:
        n_rcp = (n_alfa + n_beta * math.log(n_pz_dc_be)) * (n_pz_dc / n_pz_dc_be)
    

    return n_rcp

def perc_rcp(ativo, passivo, prazo):
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT alpha_ativo AS alpha, beta_ativo AS beta FROM alfas_betas WHERE passivo = "{ativo}" AND ativo = "{passivo}"')
    resp = pd.read_sql(filtro, conn).values

    if(resp.size == 0):
        filtro = (f'SELECT alpha_passivo AS alpha, beta_passivo AS beta FROM alfas_betas WHERE passivo = "{passivo}" AND ativo = "{ativo}"')
        resp = pd.read_sql(filtro, conn).values
    
    perc_rcp = rcp_a_b(prazo, resp[0][0], resp[0][1])

    return perc_rcp

def get_lista_ativo():
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = ("SELECT ativo FROM alfas_betas UNION SELECT passivo FROM alfas_betas")
    resp = pd.read_sql(filtro, conn).values
    lista = [x[0] for x in resp]
    return lista

def get_lista_passivo(filtro):
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro_db = (f'SELECT ativo FROM alfas_betas WHERE passivo = "{filtro}"  UNION SELECT passivo FROM alfas_betas WHERE ativo = "{filtro}"')
    resp = pd.read_sql(filtro_db, conn).values
    lista = [x[0] for x in resp]
    return lista
