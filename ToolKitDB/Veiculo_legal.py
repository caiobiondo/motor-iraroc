import pandas as pd

class Veiculo_legal:
    def __init__(self, conn):
        self.conn = conn
    
    def get_aliquota_ir_cs(self, id_veiculo_legal):
        query = ('SELECT PC_ALIQUOTA_IR_CS FROM veiculoLegal WHERE ID_VEICULO_LEGAL = %s' %id_veiculo_legal)
        aliquota = pd.read_sql(query, self.conn)
        return (aliquota.values[0][0]/100)
        