import pandas as pd
class Alfas_betas :
    def __init__(self, conn, index_passivo, index_ativo):
        self.conn = conn
        self.query = "SELECT * FROM alfas_betas WHERE (PASSIVO = '" + index_passivo + "' AND ATIVO = '" + index_ativo + "') OR (PASSIVO = '" + index_ativo + "' AND ATIVO = '" + index_passivo + "')"
        self.df = pd.read_sql(self.query, self.conn)

    def get_rebate(self):
        return self.df["rebate"].values[0]
    
    def get_alpha_ativo(self):
        return self.df["alpha_ativo"].values[0]
    
    def get_alpha_passivo(self):
        return self.df["alpha_passivo"].values[0]
    
    def get_beta_ativo(self):
        return self.df["beta_ativo"].values[0]
    
    def get_beta_passivo(self):    
        return self.df["beta_passivo"].values[0]
