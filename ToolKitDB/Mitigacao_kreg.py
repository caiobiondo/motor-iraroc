import math
import sqlite3 as sql
import pandas as pd

def mitiga_kreg_auto(json):
    conn = sql.connect('Bases/Banco_dados_att.db')
    
    valor_garanta = json["valor_garantia"]
    valor_operacao = json["valor_operacao"]
    moeda_operacao = json["moeda_operacao"]
    moeda_garantia = json["moeda_garantia"]
    produto = json["produto"]
    modalidade = json["modalidade"]
    instrumento = json["instrumento"]
    bem = json["bem"]
    natureza = json["natureza"]
    segmento = json["id_segmento"]

    filtro = (f'SELECT MITIGA_KREG FROM produto_modalidade WHERE CD_PRODUTO = "{produto}" AND CD_MODALIDADE = "{modalidade}" AND ID_SEGMENTO = {segmento}')
    filtro_garantias = (f'SELECT MITIGA_KREG FROM GARANTIAS_CEA WHERE INSTRUMENTO = "{instrumento}" AND BEM = "{bem}" AND natureza = "{natureza}" AND ID_SEGMENTO = {segmento}')
    resp_prod_mod = pd.read_sql(filtro, conn).values
    resp_garantias = pd.read_sql(filtro_garantias, conn).values

    if(len(resp_prod_mod) != 0):
        mitiga_pd_md = resp_prod_mod[0][0]
    else:
        mitiga_pd_md = "NAO"
    
    if(len(resp_garantias) != 0):
        mitiga_garantias = resp_garantias[0][0]
    else:
        mitiga_garantias = "NAO"

    if((mitiga_pd_md == "SIM") or (mitiga_garantias == "SIM")):
        filtro_he = (f'SELECT FATOR_KREG_HE FROM produto_modalidade WHERE CD_PRODUTO = "{produto}" AND CD_MODALIDADE = "{modalidade}" AND ID_SEGMENTO = {segmento}')
        filtro_hc = (f'SELECT FATOR_KREG_HC FROM GARANTIAS_CEA WHERE INSTRUMENTO = "{instrumento}" AND BEM = "{bem}" AND natureza = "{natureza}" AND ID_SEGMENTO = {segmento}')

        fator_he = float(pd.read_sql(filtro_he, conn).values[0][0])/100
        fator_hc = float(pd.read_sql(filtro_hc, conn).values[0][0])/100
        fator_fp = 1 #Nao tem descasamento de prazo
        
        if(moeda_garantia == moeda_operacao):
            fator_hfx = 0
        else:
            fator_hfx = 8/100
        
        exp_ajustada = max(0, (valor_operacao*(1 + fator_he) - (valor_garanta*(1-fator_hc-fator_hfx) * fator_fp)))
        mitigacao = 1 - (exp_ajustada/valor_operacao)

        return {"mitigacao_kreg": (mitigacao*100)}
    else:
        return {"mitigacao_kreg": 0}