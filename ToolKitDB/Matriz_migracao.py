#bibliotecas 
from Objetos.Tela import Tela
import numpy as np
import sqlite3
import pandas as pd
import math

class Matriz_migracao:
     # mes ref, id_segmento, rating_inicio, id_volatilidade, id_cenario
    def __init__(self,conn,tela):
        self.conn = conn
        self.id_volatilidade = int(tela.get_volatilidade())
        self.id_cenario = int(tela.get_id_cenario())
        ####################################################
        self.id_segmento = tela.get_id_segmento_risco_grupo()
        validos = [2, 27, 31]
        if(self.id_segmento not in validos):
            self.id_segmento = 27
        ####################################################
        self.rating_operacao = tela.get_rating_operacao()
        self.prazo_total = min(Tela.get_prazo_total(tela), 1095)
        if(self.id_volatilidade != 0):
            self.filtro = ((f'SELECT probabilidade_migracao, rating_final, mes_ref FROM matriz_migracao WHERE id_segmento = {self.id_segmento} AND id_cenario = {self.id_cenario} AND id_volatilidade = {self.id_volatilidade} AND mes_ref <= {self.prazo_total} AND rating_inicio = "{self.rating_operacao}"'))
            self.df = pd.read_sql(self.filtro, conn)
        elif(self.id_volatilidade == 0):
            self.filtro = ((f'SELECT probabilidade_migracao ,rating_final, mes_ref FROM matriz_migracao WHERE id_segmento = {self.id_segmento} AND id_cenario = {self.id_cenario} AND id_volatilidade = 1 AND mes_ref <= {self.prazo_total} AND rating_inicio = "{self.rating_operacao}"'))
            self.df = pd.read_sql(self.filtro, conn)
            self.df.loc[(self.df['rating_final'] == self.rating_operacao), 'probabilidade_migracao'] = 1
            self.df.loc[(self.df['rating_final'] != self.rating_operacao), 'probabilidade_migracao'] = 0
        
        # rating final, mes_ref, prob_migracao where mes_ref, 
    def get_migracao(self, rating_final, dias): 
        mes_ref = min(math.floor(dias/30), 36) 
        
        self.df_int = self.df.loc[(self.df.mes_ref == mes_ref) & (self.df.rating_final == rating_final)]
        probabilidade_migracao = self.df_int["probabilidade_migracao"].values

        if(len(probabilidade_migracao) == 0):
            if(self.rating_operacao == rating_final):
                probabilidade_migracao = 1
            else:
                probabilidade_migracao = 0
        else:
            probabilidade_migracao = self.df_int["probabilidade_migracao"].values[0]
            

        return probabilidade_migracao
    
    def get_migracao_np(self, rating_final, mes_ref): 
        #mes_ref = min(math.floor(dias/30), 36) 
        
        self.df_int = self.df.loc[(self.df.mes_ref == mes_ref) & (self.df.rating_final == rating_final)]
        probabilidade_migracao = self.df_int["probabilidade_migracao"].values

        if(len(probabilidade_migracao) == 0):
            if(self.rating_operacao == rating_final):
                probabilidade_migracao = 1
            else:
                probabilidade_migracao = 0
        else:
            probabilidade_migracao = self.df_int["probabilidade_migracao"].values[0]
        
        array_migracao = np.array([[probabilidade_migracao]])

        return array_migracao
