# cea_globais.xml  13/02/2020 
import pandas as pd
import numpy as np
class Cea_globais:
    def __init__(self, conn):
        self.conn = conn
        self.df = pd.read_sql("select * from cea_globais", conn)
        self.np_array = self.df.values
        self.vl_sigma = self.df['vl_sigma'].values[0]
        self.var_port = self.df['vl_var'].values[0]
        self.indice = self.df['indice'].values[0]
        self.dt_referencia = self.df['dt_referencia']
        self.vl_perda_esperada = self.df['vl_perda_esperada'].values[0]
        self.vl_intercpesos = self.df['vl_intercpesos'].values[0]
        self.vl_coefpesos = self.df['vl_coefpesos'].values[0]
        self.vl_perdawi1 = self.df['vl_perdawi1'].values[0]
        self.vl_fatork1 = self.df['vl_fatork1'].values[0]
        self.vl_fatork2 = self.df['vl_fatork2'].values[0]
        #self.vl_fatork3 = self.df['vl_fatork3'].values[0]
        self.vl_fatork3 = 2.72
        self.vl_var = self.df['vl_var'].values[0]
        self.vl_desvport = self.df['vl_desvport'].values[0]
        self.vl_bis = self.df['vl_bis'].values[0]
        self.vl_varktot = self.df['vl_varktot'].values[0]
        self.vl_fatmdpz = self.df['vl_fatmdpz'].values[0]
        self.vl_pehbacen = self.df['vl_pehbacen'].values[0]
        self.vl_pemodelo = self.df['vl_pemodelo'].values[0]
        self.vl_peprazo = self.df['vl_peprazo'].values[0]
        self.vl_aliquota = self.df['vl_aliquota'].values[0]
        self.seg_emp = self.df['seg_emp'].values[0]
        self.ds_filler = self.df['ds_filler'].values[0]

    def get_pe_port(self):
        return self.vl_perda_esperada
    def get_sigma_port(self):
        return self.vl_sigma
    def get_pewi_port(self):
        return self.vl_perdawi1
    def get_k1_port(self):
        return self.vl_fatork1
    def get_k2_port(self):
        return self.vl_fatork2
    def get_k3_port(self):
        return self.vl_fatork3
    def get_var_port(self):
        return self.vl_var
    def get_desvio_port(self):
        return self.vl_desvport
