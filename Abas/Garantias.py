import pandas as pd
import numpy as np
import sqlite3 as sql

from ToolKitDB.RCP import *

def get_instrumento_garantias(id_segmento):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT DISTINCT INSTRUMENTO FROM GARANTIAS_CEA WHERE ID_SEGMENTO = {id_segmento}')
    array_instrumentos = pd.read_sql(filtro, conn).values
    for i in range(len(array_instrumentos)):
        lista.append(array_instrumentos[i][0]) 
    return {"instrumentos":lista}

def get_bem_garantias(id_segmento, instrumento):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT DISTINCT(BEM) FROM GARANTIAS_CEA WHERE INSTRUMENTO = "{instrumento}" AND ID_SEGMENTO = {id_segmento}')
    array_bens = pd.read_sql(filtro, conn).values
    for i in range(len(array_bens)):
        lista.append(array_bens[i][0]) 
    return {"bem":lista}

def get_natureza_garantias(id_segmento, instrumento, bem):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT DISTINCT(NATUREZA) FROM GARANTIAS_CEA WHERE INSTRUMENTO = "{instrumento}" AND BEM = "{bem}" AND ID_SEGMENTO = {id_segmento}')
    array_natureza = pd.read_sql(filtro, conn).values
    for i in range(len(array_natureza)):
        lista.append(array_natureza[i][0]) 
    return {"natureza":lista}

def get_parametros_garantias(id_segmento, instrumento, bem, natureza):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
   
    filtro = (f'SELECT PERC_HE, HV, LMM,UTILIZAR FROM GARANTIAS_CEA WHERE INSTRUMENTO = "{instrumento}" AND BEM = "{bem}" AND natureza = "{natureza}" AND ID_SEGMENTO = {id_segmento}')
    array_parametros = pd.read_sql(filtro, conn).values[0]
    for i in range(len(array_parametros)):
        lista.append(array_parametros[i])
    return {"parametros":{"perc_he": int(lista[0]), "hv": int(lista[1]), "lmm": int(lista[2]), "mitiga": lista[3]}} 

def mitigacao_garantias(valor_operacao, valor_garantia, moeda_garantia, moeda_operacao, hv, he, lmm, prazo):
    if(moeda_garantia == moeda_operacao):
        cobertura = valor_garantia / valor_operacao
        
        calculado = (he/100) * (hv/100) * cobertura
        mitigado = min([lmm/100, (he/100), calculado]) * 100

        return {"resultado_mit_pe":mitigado, "resultado_mit_cea":mitigado}  
    else:
        rcp_cea =  perc_rcp(moeda_operacao, moeda_garantia, prazo)
        rcp_garantia = rcp_cea/0.224
        hv_cea = (hv * (1 + rcp_cea))/(1 + rcp_garantia)
        
        cobertura = valor_garantia / (valor_operacao * (1 + rcp_cea))

        calculado = (he/100) * (hv_cea/100) * cobertura
        resultado_cea = min([lmm/100, (he/100), calculado]) * 100

        resultado_cea_2 = calculado * 100

        return{"resultado_mit_pe":resultado_cea, "resultado_mit_cea":resultado_cea_2}