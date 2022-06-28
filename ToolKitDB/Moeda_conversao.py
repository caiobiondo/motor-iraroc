#bibliotecas 
import sqlite3
import pandas as pd
import math
import os

# MOEDA_CONVERSAO.xml 13/02/2020 
class Moeda_conversao:
    def __init__(self, conn, moeda):
        self.conn = conn
        self.df = pd.read_sql("select * from moeda_conversao", conn)
        self.moeda = moeda
        self.ds_moeda = self.df['ds_moeda']
        self.id_moeda = self.df['id_moeda']
        self.vl_spot = self.df['vl_spot']

    def get_spot(self):      
        self.df_int = self.df.loc[(self.ds_moeda == self.moeda)]
        return self.df_int["vl_spot"].values[0]