import sqlite3
import pandas as pd
from ToolKitDB.Regua_pd import Regua_pd
from Objetos.Tela import Tela
import numpy as np

#PEGAR PRODUTO E MODALIDADE DAS OPERACOES DENTRO DO ESTOQUE
class Operacao_df:
    def __init__(self,regua_pd,df_operacoes):
        if(df_operacoes.empty):
            self.regua_pd = regua_pd
            self.df_operacoes = df_operacoes
        else:
            self.df_operacoes = df_operacoes
            self.df_operacoes['rating_original'] = self.df_operacoes['rating']
            self.df_operacoes['incluir_no_cluster'] = False
            self.regua_pd = regua_pd
            self.df_operacoes["pd_modelo"] = np.vectorize(self.regua_pd.get_pd_modelo)(self.df_operacoes['id_segmento_risco_grupo'], self.df_operacoes['rating'])
            self.df_operacoes["vol_pd_modelo"] = np.vectorize(self.regua_pd.get_vol_pd_modelo)(self.df_operacoes['id_segmento_risco_grupo'], self.df_operacoes['rating'])
            self.df_operacoes["pd_roe"] = self.df_operacoes["pd_modelo"]
            self.df_operacoes["vol_pd_roe"] = self.df_operacoes["vol_pd_modelo"]
            self.df_operacoes["piso_cea"] = 0
            self.df_operacoes["wi"] = np.vectorize(self.regua_pd.get_wi)(self.df_operacoes['id_segmento_risco_grupo'], self.df_operacoes['rating'])
            self.df_operacoes["default"] = np.vectorize(self.regua_pd.is_rating_default)(self.df_operacoes['id_segmento_risco_grupo'], self.df_operacoes['rating'])
            self.df_operacoes["lgd_be"] = np.vectorize(self.regua_pd.get_lgd_be)(self.df_operacoes['id_segmento_risco_grupo'], self.df_operacoes['rating'])
            self.df_operacoes["lgd_dt"] = np.vectorize(self.regua_pd.get_lgd_dt)(self.df_operacoes['id_segmento_risco_grupo'], self.df_operacoes['rating'])
            self.df_operacoes["is_vencida"] = False
            self.df_operacoes["condicao_mudar_rtg"] = False
       
    def is_vencida_df (self,data_ref):
        self.df_operacoes["is_vencida"] = self.df_operacoes["duration"].le(data_ref)            

    def mudar_rating (self,rating): 
        self.df_operacoes.loc[self.df_operacoes['condicao_mudar_rtg'], 'rating'] = rating

    def mudar_lgd(self,multiplicador):
        self.df_operacoes["lgd"] = self.df_operacoes["lgd"] * multiplicador
    
    def mudar_prazo(self,multiplicador):
        self.df_operacoes["duration"] = self.df_operacoes["duration"] * multiplicador

    def set_incluir_no_cluster(self,valor):
        self.df_operacoes["incluir_no_cluster"] = valor

    def resetar_rating(self):
        self.df_operacoes["rating"] = self.df_operacoes["rating_original"]
        
    def get_incluir_no_cluster(self):
        return self.df_operacoes["incluir_no_cluster"]
    
    def get_lgd(self):
        return self.df_operacoes["lgd"]
        
    def get_cnpj_cabeca(self):
        return self.cnpj_cabeca

    def get_id_grupo(self):
        return self.df_operacoes['id_segmento_risco_grupo'].values[0]

    def get_rating(self):
        return self.df_operacoes["rating"]

    def get_pd_roe(self):
        return self.pd_roe

    def get_cd_prod(self):
        return self.cd_prod

    def get_cd_mod(self):
        return self.cd_mod
    
    def get_pd_modelo(self):
        return self.df_operacoes["pd_modelo"]

    def get_rating_original(self):
        return self.df_operacoes["rating_original"]

    def get_segmento_risco_cabeca(self):
        return self.id_segmento_risco_cabeca

    def get_segmento_risco_grupo(self):
        return self.id_segmento_risco_grupo

    def get_ead(self):
        return self.df_operacoes["ead"]

    def get_ead_original(self):
        return self.df_operacoes["ead_original"]
    
    def get_vol_pd_modelo(self):
        return self.vol_pd_modelo
    
    def get_pd_modelo(self):
        return self.df_operacoes["pd_modelo"]

    def get_wi(self):
        return self.df_operacoes["wi"]

    def get_piso_cea(self):
        return self.df_operacoes["piso_cea"]

    def get_id_volatilidade(self):
        return self.id_volatilidade

    def get_lgd_be(self):
        return self.df_operacoes["lgd_be"]

    def get_lgd_dt(self):
        return self.df_operacoes["lgd_dt"]

    def is_default(self):
        return self.df_operacoes["default"]

    def get_duration(self):
        return self.df_operacoes["duration"]

    def is_in_carteira(self):
        return self.esta_na_carteira

    def set_ead(self,ead_novo):
        self.df_operacoes["ead"] = ead_novo      #acessar apenas a nova operacao
     
    def get_spread(self):
        return self.spread