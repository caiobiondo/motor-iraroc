import numpy as np
import pandas as pd
class MRC:
    def __init__(self, conn):
        self.conn = conn
        self.query = "SELECT MRC, cd_quebra, cluster FROM RAROC_QUEBRAS_MRC " 
        self.df = pd.read_sql(self.query, self.conn)
    
    def get_quebra_mrc(self, cd_quebra, cluster):
        np_mrc = self.df.values
        array_mrc = np_mrc[:,0]
        array_cd_quebra = np_mrc[:,1]
        array_cluster = np_mrc[:,2]
        mrc = np.where((array_cd_quebra == array_cd_quebra) & (array_cluster == cluster),array_mrc, 0)
        mrc_filtro = np.delete(mrc, np.where(mrc == 0), axis = 0)
        if(mrc_filtro.size == 0):
            return 0
        else:
            return mrc_filtro[0]