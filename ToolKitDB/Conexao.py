#bibliotecas 
import sqlite3
import pandas as pd
import math

class Conexao:  
    def consulta_bd(self,query,conexao):  
        self.query = query
        self.coneccao = conexao   
        return pd.read_sql(query, conexao)

    