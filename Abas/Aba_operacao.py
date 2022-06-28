import pandas as pd
import numpy as np
import sqlite3 as sql

def get_produtos_segmento(segmento):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT DISTINCT CD_PRODUTO FROM produto_modalidade WHERE ID_SEGMENTO = "{segmento}"')
    array_produtos = pd.read_sql(filtro, conn).values
    for i in range(len(array_produtos)):
        lista.append(array_produtos[i][0]) 
    return {"produtos":lista}

def get_modalidades(segmento, produto):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT DISTINCT CD_MODALIDADE FROM produto_modalidade WHERE CD_PRODUTO = "{produto}" AND ID_SEGMENTO = "{segmento}"')
    array_modalidade = pd.read_sql(filtro, conn).values
    for i in range(len(array_modalidade)):
        lista.append(array_modalidade[i][0]) 
    return {"modalidades":lista}

def get_infos_produto(produto, modalidade, segmento):
    lista = []
    conn = sql.connect('Bases/Banco_dados_att.db')
    filtro = (f'SELECT * FROM produto_modalidade WHERE CD_PRODUTO = "{produto}" AND CD_MODALIDADE = "{modalidade}" AND ID_SEGMENTO = "{segmento}"')
    array_infos = pd.read_sql(filtro, conn).values[0]
    
    return {
    "dt_inicio_vigencia": (array_infos[0]),
    "dt_fim_vigencia": (array_infos[1]),
    "k_receitas": (array_infos[2]),
    "k_custos": (array_infos[3]),
    "nr_dias_desembolso":(array_infos[4]),
    "pc_mitg_lgd": (array_infos[5]),
    "pc_lgd":(array_infos[6]),
    "cd_prz_ind": (array_infos[7]),
    "vl_prz_calc":(array_infos[8]),
    "pc_mitg_ead_cea": (array_infos[9]),
    "pc_mitg_ead_kreg": (array_infos[10]),
    "id_veiculo_legal":(array_infos[11]),
    "id_segmento": (array_infos[12]),
    "pc_ccf": (array_infos[13]),
    "cd_produto": (array_infos[14]),
    "cd_modalidade": (array_infos[15]),
    "ic_onshore": (array_infos[16]),
    "ic_prz_medio": (array_infos[17]),
    "ic_funding": (array_infos[18]),
    "cd_tipo_taxa":(array_infos[19]),
    "ic_aplic_iss": (array_infos[20]),
    "cd_comp_produto": (array_infos[21]),
    "pc_ccf_onshore_l1y_ig":(array_infos[22]),
    "pc_ccf_onshore_l1y_hy":(array_infos[23]),
    "pc_ccf_onshore_m1y_ig": (array_infos[24]),
    "pc_ccf_onshore_m1y_hy": (array_infos[25]),
    "pc_ccf_offshore_l1y_ig": (array_infos[26]),
    "pc_ccf_offshore_l1y_hy": (array_infos[27]),
    "pc_ccf_offshore_m1y_ig":(array_infos[28]),
    "pc_ccf_offshore_m1y_hy": (array_infos[29]),
    "tp_mod_rcp": (array_infos[30]),
    "tp_oper": (array_infos[31]),
    "pc_ccf_roe_l1y_ig":(array_infos[32]),
    "pc_ccf_roe_l1y_hy":(array_infos[33]),
    "pc_ccf_roe_m1y_ig":(array_infos[34]),
    "pc_ccf_roe_m1y_hy": (array_infos[35]),
    "ds_veiculo_legal": (array_infos[36]),
    "ds_segmento": (array_infos[37]),
    "id_produto":(array_infos[38]),
    "id_familia": (array_infos[39]),
    "mitiga_lgd": (array_infos[40]),
    "mitiga_kreg": (array_infos[41]),
    "fator_kreg_he": (array_infos[42])
  }

