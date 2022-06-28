import pandas as pd
import math
import sqlite3

# fepf consulta banco de dados chave cod classe e valor prazo, pega valor fepf
def get_fepf(prazo_anos, tipo_derivativo, connDB):
    if(prazo_anos == 0):
        return 0
    elif(prazo_anos <= 1):
        filtro = (f'SELECT vl_fepf FROM fepf WHERE cod_clas_diretiva = "{tipo_derivativo}" AND vl_pz_oper == 359')
        return pd.read_sql(filtro, connDB).values[0][0]
    elif (prazo_anos > 1 and prazo_anos <= 5):
        filtro = (f'SELECT vl_fepf FROM fepf WHERE cod_clas_diretiva = "{tipo_derivativo}" AND vl_pz_oper == 1800')
        return pd.read_sql(filtro, connDB).values[0][0]
    else:
        filtro = (f'SELECT vl_fepf FROM fepf WHERE cod_clas_diretiva = "{tipo_derivativo}" AND vl_pz_oper == {prazo_anos}')
        return pd.read_sql(filtro, connDB).values[0][0]

