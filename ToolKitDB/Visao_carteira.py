import sqlite3
import pandas as pd
import math

# VISAO_CARTEIRA.xml         
class Visao_carteira:
    def __init__(self,id_grupo, conn):
        self.conn = conn
        self.df = pd.read_sql( "SELECT id_grupo,num_raroc_full_vp,cea_vp,num_roe_full_vp,cr_pl_vp,prazomedio_inicial,ead_vp FROM visao_carteira WHERE ID_GRUPO = '" + str(id_grupo) + "'", conn)
        if(self.df.empty):
            self.id_grupo = id_grupo
            self.prazomedio_inicial = 1
            self.num_raroc_full_vp = 0
            self.cea_vp = 0
            self.num_roe_full_vp = 0
            self.cr_pl_vp = 0
            self.ead_vp = 0
        else:
            self.id_grupo = self.df['id_grupo'].values[0]
            self.num_raroc_full_vp = self.df['num_raroc_full_vp'].values[0]
            self.cea_vp = self.df['cea_vp'].values[0]
            self.num_roe_full_vp = self.df['num_roe_full_vp'].values[0]
            self.cr_pl_vp = self.df['cr_pl_vp'].values[0]
            self.prazomedio_inicial = self.df['prazomedio_inicial'].values[0]
            self.ead_vp = self.df['ead_vp'].values[0]
         
    def get_num_raroc(self):
        return self.num_raroc_full_vp
    def get_cea_vp(self):
        return self.cea_vp
    def get_num_roe(self):
        return self.num_roe_full_vp
    def get_cr_vp(self):
        return self.cr_pl_vp
    def get_prazo_medio(self):
        return self.prazomedio_inicial
    def get_ead_vp(self):
        return self.ead_vp

    def get_pd_modelo(self,id_grupo,ativo):
        self.df_int = self.df.loc[(self.id_grupo == id_grupo) & (self.df.ativo == ativo)]
        return self.df_int["pd_modelo"].values[0]
    
        
        