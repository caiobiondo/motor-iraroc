import pandas as pd
import numpy as np
import sqlite3 as sql

def get_infos_risco(id_grupo):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT CNPJ_CABECA, NM_SEGMENTO_RISCO_GRUPO, ID_SEGMENTO_RISCO_CABECA FROM IDGRUPO_SEGTO_RISCO WHERE ID_GRUPO = "{id_grupo}"')
    resp = pd.read_sql(filtro, conn)
    cnpj = str(resp.values[0][0])
    segto_risco = resp.values[0][1]
    id_segto_risco = resp.values[0][2]
    return {"cnpj": cnpj, "segmento_risco": segto_risco, "id_segto_risco":id_segto_risco}

def get_parametros_volatilidade(id_grupo):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro_vol = (f'SELECT ds_volatilidade, id_volatilidade, id_grupo FROM parametros_estrategia_portfolio WHERE id_grupo = "{id_grupo}"')
    resp_vol = pd.read_sql(filtro_vol, conn)
    vol = str(resp_vol.values[0][0])
    id_vol = resp_vol.values[0][1]
    id_grupo = resp_vol.values[0][2]
    return {"id_grupo": id_grupo, "ds_volatilidade": vol, "id_volatilidade":id_vol}
