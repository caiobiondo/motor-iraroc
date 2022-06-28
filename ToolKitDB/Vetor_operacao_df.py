from ToolKitDB.Operacao_df import Operacao_df
import pandas as pd
import numpy as np
from ToolKitDB.Regua_pd import Regua_pd
from Objetos.Tela import Tela

#MUDAR DATAFRAME DE RESPOSTA PARA NAO GERAR MODELO DUPLICADO (AUMENTA TAMANHO DOS VETORES)
def get_operacoes_vazio(connDB):
    regua_pd = Regua_pd(connDB)
    operacoes_vazio = pd.DataFrame()
    operacoes = Operacao_df(regua_pd, operacoes_vazio)
    return operacoes

def get_operacoes_df(cnpj_cab,volatilidade, tela, connDB):

    filtro = (f"SELECT * FROM base_operacoes  WHERE cd_cnpj = '{cnpj_cab}'")
    regua_pd = Regua_pd(connDB)
    res = pd.read_sql(filtro, connDB)
    res_filtrado = pd.DataFrame()
    df_operacoes = pd.DataFrame()

    if(res.empty):
        operacoes_vazio = pd.DataFrame()
        operacoes = Operacao_df(regua_pd, operacoes_vazio)
    else:
        res["cnpj_cabeca"] = res["cd_cnpj"]
        res["rating"] = res["rtg_op"].astype(str)
        res["id_segmento_risco_grupo"] = res["id_segmento_grupo"].astype(int)
        res["id_segmento_risco_cabeca"] = res["id_segmento_cnpj"].astype(int)
        res["lgd"] = Tela.get_lgd_mitigada(tela)
        res["ead"] = res["vl_ead_efetiva"].astype(float)
        res["duration"] = res["prz_medio"].astype(float)
        res["spread"] = Tela.get_spread(tela)
        res["spread"] = res["spread"].astype(float) 
        res["volatilidade"] = str(volatilidade)
        res["esta_na_carteira"] = True
        res["esta_na_carteira"].astype(bool)
        res["pd_modelo"] = np.vectorize(regua_pd.get_pd_modelo)(res['id_segmento_risco_grupo'].values[0], res['rating'].values[0])
        res["wi"] = np.vectorize(regua_pd.get_wi)(res["id_segmento_risco_grupo"].values[0], res["rtg_op"].values[0])
        res["produto"] = res["cd_prod"].astype(str)
        res["modalidade"] = res["cd_mod"].astype(str)

        #TRATAMENTO DE DATAS
        res["data_vencimento"] = pd.to_datetime(res["dt_vencimento"])
        res["data_vencimento"] = pd.to_datetime(res["data_vencimento"].dt.strftime("%Y-%m"))
        
        res = res.drop(res[res['ead'] == 0].index)
        res = res.drop(res[res['lgd'] == 0].index)
        
        #CONSOLIDAR GRUPOS
        res_filtrado = res[['cd_mod','id_grupo', 'cd_prod', 'vl_lgd', 'cnpj_cabeca', 'rating','id_segmento_risco_grupo', 'id_segmento_risco_cabeca', 'lgd', 'ead',
        'duration', 'spread', 'volatilidade', 'esta_na_carteira', 'pd_modelo', 'dt_abertura', 'data_vencimento', "produto", 'modalidade', "vl_ead_efetiva", "rtg_op"]]

        consolidado = res_filtrado.groupby(["data_vencimento", "id_grupo", "rtg_op"]).apply(lambda x: ajuste(x)).values
            
        for i in range(len(consolidado)):
            df_operacoes = df_operacoes.append(consolidado[i], ignore_index=True)

        operacoes = Operacao_df(regua_pd, df_operacoes)

    return operacoes

def get_operacoes_df_cluster(cnpj_cab,volatilidade, tela, connDB):
    try:
        filtro = (f"SELECT * FROM base_operacoes  WHERE cd_cnpj = '{cnpj_cab}'")
        regua_pd = Regua_pd(connDB)
        res = pd.read_sql(filtro, connDB)
        res_filtrado = pd.DataFrame()
        df_operacoes = pd.DataFrame()

        if(res.empty):
            operacoes_vazio = pd.DataFrame()
            operacoes = Operacao_df(regua_pd, operacoes_vazio)
        else:
            res["cnpj_cabeca"] = res["cd_cnpj"]
            res["rating"] = res["rtg_op"].astype(str)
            res["id_segmento_risco_grupo"] = res["id_segmento_grupo"].astype(int)
            res["id_segmento_risco_cabeca"] = res["id_segmento_cnpj"].astype(int)
            res["lgd"] = res["vl_lgd_mitigada"]
            res["ead"] = res["vl_ead_efetiva"].astype(float)
            res["duration"] = res["prz_medio"].astype(float)
            res["spread"] = Tela.get_spread(tela)
            res["spread"] = res["spread"].astype(float) 
            res["volatilidade"] = str(volatilidade)
            res["esta_na_carteira"] = True
            res["esta_na_carteira"].astype(bool)
            res["pd_modelo"] = np.vectorize(regua_pd.get_pd_modelo)(res['id_segmento_risco_grupo'].values[0], res['rating'].values[0])
            res["wi"] = np.vectorize(regua_pd.get_wi)(res["id_segmento_risco_grupo"].values[0], res["rtg_op"].values[0])
            res["produto"] = res["cd_prod"].astype(str)
            res["modalidade"] = res["cd_mod"].astype(str)

            #TRATAMENTO DE DATAS
            res["data_vencimento"] = pd.to_datetime(res["dt_vencimento"])
            res["data_vencimento"] = pd.to_datetime(res["data_vencimento"].dt.strftime("%Y-%m"))
            
            res = res.drop(res[res['ead'] == 0].index)
            res = res.drop(res[res['lgd'] == 0].index)
            
            #CONSOLIDAR GRUPOS
            res_filtrado = res[['cd_mod','id_grupo', 'cd_prod', 'vl_lgd', 'cnpj_cabeca', 'rating','id_segmento_risco_grupo', 'id_segmento_risco_cabeca', 'lgd', 'ead',
            'duration', 'spread', 'volatilidade', 'esta_na_carteira', 'pd_modelo', 'dt_abertura', 'data_vencimento', "produto", 'modalidade', "vl_ead_efetiva", "rtg_op"]]

            consolidado = res_filtrado.groupby(["data_vencimento", "id_grupo", "rtg_op"]).apply(lambda x: ajuste(x)).values
                
            for i in range(len(consolidado)):
                df_operacoes = df_operacoes.append(consolidado[i], ignore_index=True)

            operacoes = Operacao_df(regua_pd, df_operacoes)

        return operacoes
    except:
        return {"EAD_CLUSTER": "-", "CEA_CLUSTER": "-", "MRC":"-", "PZR": "-", "LGD": "-", "NOME": "-"}

def gerar_nova_op(id_grupo, vl_lgd, cd_mod, cd_prod, cnpj, rating, id_seg_risco_grupo, id_seg_risco_cabeca, lgd, ead, duration, spread, volatilidade, regua_pd):
   nova_op = pd.DataFrame([[str(cd_mod), int(id_grupo), str(cd_prod),float(vl_lgd), str(cnpj), str(rating), int(id_seg_risco_grupo), int(id_seg_risco_cabeca), float(lgd), float(ead), float(duration), float(spread),  str(volatilidade), False]], 
   columns= ['cd_mod', 'id_grupo', 'cd_prod', 'vl_lgd', 'cnpj_cabeca', 'rating',
       'id_segmento_risco_grupo', 'id_segmento_risco_cabeca', 'lgd', 'ead',
       'duration', 'spread', 'volatilidade', 'esta_na_carteira'], index=[-1])

   
   nova_op['pd_modelo'] = Regua_pd.get_pd_modelo(regua_pd, id_seg_risco_grupo, rating)
   nova_op['default'] = Regua_pd.is_rating_default(regua_pd, id_seg_risco_grupo, rating)
   nova_op['wi'] = Regua_pd.get_wi(regua_pd, id_seg_risco_grupo, rating)
   nova_op['lgd_be'] = Regua_pd.get_lgd_be(regua_pd, id_seg_risco_grupo, rating)
   nova_op['lgd_dt'] = Regua_pd.get_lgd_dt(regua_pd, id_seg_risco_grupo, rating) 
   nova_op['condicao_mudar_rtg'] = False
   nova_op['is_vencida'] = False
   nova_op['rating_original'] = rating
   nova_op['incluir_no_cluster'] = False
   return nova_op

def set_duration_nova(nova, duration):
    nova["duration"] = duration

def gerar_nova_op_cluster(id_grupo, vl_lgd, cd_mod, cd_prod, cnpj, rating, id_seg_risco_grupo, id_seg_risco_cabeca, lgd, ead, duration, spread, volatilidade, regua_pd):   
   nova_op = pd.DataFrame([[str(cd_mod), int(id_grupo), str(cd_prod),float(vl_lgd), str(cnpj), str(rating), int(id_seg_risco_grupo), int(id_seg_risco_cabeca), float(lgd), float(ead), float(duration), float(spread),  str(volatilidade), False]], 
   columns= ['cd_mod', 'id_grupo', 'cd_prod', 'vl_lgd', 'cnpj_cabeca', 'rating',
       'id_segmento_risco_grupo', 'id_segmento_risco_cabeca', 'lgd', 'ead',
       'duration', 'spread', 'volatilidade', 'esta_na_carteira'], index=[-1])

   
   nova_op['pd_modelo'] = Regua_pd.get_pd_modelo(regua_pd, id_seg_risco_grupo, rating)
   nova_op['default'] = Regua_pd.is_rating_default(regua_pd, id_seg_risco_grupo, rating)
   nova_op['wi'] = Regua_pd.get_wi(regua_pd, id_seg_risco_grupo, rating)
   nova_op['lgd_be'] = Regua_pd.get_lgd_be(regua_pd, id_seg_risco_grupo, rating)
   nova_op['lgd_dt'] = Regua_pd.get_lgd_dt(regua_pd, id_seg_risco_grupo, rating) 
   nova_op['condicao_mudar_rtg'] = False
   nova_op['is_vencida'] = False
   nova_op['rating_original'] = rating
   nova_op['incluir_no_cluster'] = True
   return nova_op

def troca_spread_nova_op(nova_op, novo_spread):
    nova_op['spread'] = float(novo_spread)

def ajuste(df):
    lgd_ajustada =  (sum(df['lgd'] * df['vl_ead_efetiva']))/sum(df['vl_ead_efetiva'])
    resp = {
    "modalidade":df["modalidade"].values[0],
    "id_grupo":df["id_grupo"].values[0],
    "produto":df["produto"].values[0],
    "cnpj_cabeca":df["cnpj_cabeca"].values[0],
    "rating":df["rating"].values[0],
    'id_segmento_risco_grupo':df['id_segmento_risco_grupo'].values[0],
    'id_segmento_risco_cabeca':df['id_segmento_risco_cabeca'].values[0],
    'spread':df['spread'].values[0],
    'volatilidade':df['volatilidade'].values[0],
    'esta_na_carteira':df['esta_na_carteira'].values[0],
    'dt_abertura':df['dt_abertura'].values[0],
    'data_vencimento':df['data_vencimento'].values[0],
    "duration": (sum(df["duration"] * df["vl_ead_efetiva"] * df["vl_lgd"]))/(sum(df["vl_ead_efetiva"] * df["vl_lgd"])),
    "lgd": sum(df['lgd']*df['vl_ead_efetiva'])/sum(df['vl_ead_efetiva']),
    "vl_lgd": (sum(df['lgd'] * df['vl_ead_efetiva']))/sum(df['vl_ead_efetiva']),
    "pd_modelo":(sum(df['pd_modelo'] * df['vl_ead_efetiva'] * lgd_ajustada)) / (sum(df["vl_ead_efetiva"] * lgd_ajustada)),
    "ead" : sum(df['vl_ead_efetiva'])
    }    
    return resp
