#bibliotecas 
import numpy as np
import sqlite3
import pandas as pd
import time
class Regua_pd:
    
    def __init__(self,conn):
        
        self.df = pd.read_sql("SELECT id_segmento,rating,pd_modelo,vol_pd_modelo,wi,piso_cea, is_default,lgd_be,lgd_dt, pd_roe, vol_pd_roe FROM vol_wi_piso_cea", conn)
        self.regua_np = self.df.values
        
    def get_pd_modelo(self,id_segmento,rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        return float(self.df_int["pd_modelo"].values[0])
    
    def get_vol_pd_modelo(self,id_segmento,rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        return float(self.df_int["vol_pd_modelo"].values[0])
    
    def get_wi(self,id_segmento,rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        return float(self.df_int["wi"].values[0])
    
    def get_piso_cea(self,id_segmento,rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        #return self.df_int["piso_cea"].values[0]
        return 0    
    
    def is_rating_default(self,id_segmento,rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        return self.df_int["is_default"].values[0] 
    
    def get_lgd_be(self,id_segmento,rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        return float(self.df_int["lgd_be"].values[0]) 
    
    def get_lgd_dt(self,id_segmento,rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        return float(self.df_int["lgd_dt"].values[0])  

    def get_pd_roe(self,id_segmento,rating):
          self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
          return float(self.df_int["pd_roe"].values[0])
    
    def get_vol_pd_roe(self, id_segmento, rating):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento) & (self.df.rating == rating)]
        return float(self.df_int["vol_pd_roe"].values[0])
    
    def get_array_np(self, id_segmento):
        self.df_int = self.df.loc[(self.df.id_segmento == id_segmento)]
        return self.df_int[["rating", "pd_modelo", "wi", "lgd_dt", "lgd_be", "is_default"]].values

    
