import pandas as pd
import numpy as np
import sqlite3 as sql
import logging

def get_fpr_cnpj(id_segmento_risco_grupo):
    if ( (id_segmento_risco_grupo == "5") or 
    (id_segmento_risco_grupo == "14") or
    (id_segmento_risco_grupo == "15") or
    (id_segmento_risco_grupo == "16") or
    (id_segmento_risco_grupo == "17") or
    (id_segmento_risco_grupo == "18") or
    (id_segmento_risco_grupo == "19") or
    (id_segmento_risco_grupo == "20") or
    (id_segmento_risco_grupo == "21") or
    (id_segmento_risco_grupo == "22") or
    (id_segmento_risco_grupo == "23") or
    (id_segmento_risco_grupo == "24") or
    (id_segmento_risco_grupo == "25") or
    (id_segmento_risco_grupo == "29") or
    (id_segmento_risco_grupo == "99")):
        return 0.5
    else:
        return 0.85

def get_fpr_novo(conn, cnpj, fpr_default):
    filtro = (f'SELECT fpr_pos FROM fpr WHERE cnpj = {cnpj} ')
    fpr_banco = pd.read_sql(filtro, conn).values
    if(len(fpr_banco) == 0):
        valor_fpr = float(fpr_default)
        fpr = str(valor_fpr*100)
    else:
        valor_fpr = float(fpr_banco[0][0])
        fpr = str(valor_fpr*100)
    
    return fpr

def get_fpr_pre_pos(cnpj):
    conn = sql.connect('Bases/fpr.db')
    filtro = (f'SELECT fpr_pre, fpr_pos FROM fpr WHERE cnpj = "{cnpj}" ')
    df_fpr = pd.read_sql(filtro, conn)
    
    if(len(df_fpr.values) == 0):
        fpr_pre = 1.0
        fpr_pos = 1.0
    else:
        fpr_pre = float(df_fpr["fpr_pre"].values[0])
        fpr_pos = float(df_fpr["fpr_pos"].values[0])
        
    resposta = {"fpr_pre":fpr_pre, "fpr_pos":fpr_pos}
    return resposta

def get_fpr_pos(cnpj, conn):
    #conn = sql.connect('Bases/fpr.db')
    filtro = (f'SELECT fpr_pos FROM fpr WHERE cnpj = "{cnpj}" ')
    df_fpr = pd.read_sql(filtro, conn)
    
    if(len(df_fpr.values) == 0):
        fpr_pos = 1.0
    else:
        fpr_pos = float(df_fpr["fpr_pos"].values[0])
        
    resposta = fpr_pos

    return resposta

def get_lista_fpr():
    lista_fpr_pre = []
    lista_fpr_pos = []

    obj_fpr_pre = {}
    obj_fpr_pos = {}

    fpr_pre = [0,20,25,50,70,85,100]
    fpr_pos = [0,20,25,30,35,40,45,50,60,65,70,80,85,90,100,105,110,130,150]
   
    for i in range(len(fpr_pre)):
        lista_fpr_pre.append(fpr_pre[i])
    
    for i in range(len(fpr_pos)):
        lista_fpr_pos.append(fpr_pos[i])

    
    return {"fpr_pre": lista_fpr_pre, "fpr_pos": lista_fpr_pos}