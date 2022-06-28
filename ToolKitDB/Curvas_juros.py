import sqlite3
import pandas as pd
import math

class Curvas_juros:
    def __init__(self, conn, prazo_total, indexador, indexador_garantia = "BRL"):
        self.conn = conn
        self.prazo = max(prazo_total, 1100)
        self.indexador = indexador
        self.indexador_garantia = indexador_garantia
        self.filtro = (f'SELECT * FROM curvas_juros WHERE (des_curva = "BRL" OR des_curva = "SELIC_ECONOMICA" OR des_curva = "JCP" OR des_curva = "IPCA" OR des_curva = "FLAT" OR des_curva = "{self.indexador}" OR des_curva = "{self.indexador_garantia}") AND dias_corridos <= {self.prazo}')
        self.df = pd.read_sql(self.filtro, conn)
        
        
    def get_curva_dc(self,des_curva,dias_corridos):
        self.df_int = self.df.loc[(self.df.des_curva == des_curva) & (self.df.dias_corridos == dias_corridos)]
        return self.df_int["fator_acumulado"].values[0]
       
    def get_curva_du(self,des_curva,dias_uteis):
        self.df_int = self.df.loc[(self.df.des_curva == des_curva) & (self.df.dias_uteis == dias_uteis)]
        return self.df_int["fator_acumulado"].values[0]
    
    def get_du_from_dc(self,dias_corridos):
        self.df_int = self.df.loc[(self.df.des_curva == "BRL") & (self.df.dias_corridos == dias_corridos)]
        return self.df_int["dias_uteis"].values[0]
    
    def get_perc_jcp(self,dias_corridos):
        self.df_int = self.df.loc[(self.df.des_curva == "JCP") & (self.df.dias_corridos == dias_corridos)]
        return self.df_int["fator_acumulado"].values[0]

       