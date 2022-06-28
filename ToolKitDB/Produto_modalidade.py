import pandas as pd

class Produto_modalidade :
    def __init__(self,id_segmento,produto,modalidade, conn):
     
        self.query = "SELECT * FROM produto_modalidade WHERE ID_SEGMENTO = '" +str(id_segmento) + "' AND UPPER(TRIM(CD_PRODUTO)) = UPPER('" + str(produto) + "') AND UPPER(TRIM(CD_MODALIDADE)) = UPPER('" + str(modalidade) + "')"
        
        self.df = pd.read_sql (self.query, conn)
        
        self.pc_ccf_onshore_l1y_ig = self.df['pc_ccf_onshore_l1y_ig'].values[0]
        self.pc_ccf_onshore_l1y_hy = self.df['pc_ccf_onshore_l1y_hy'].values[0]
        self.pc_ccf_onshore_m1y_ig = self.df['pc_ccf_onshore_m1y_ig'].values[0]
        self.pc_ccf_onshore_m1y_hy = self.df['pc_ccf_onshore_m1y_hy'].values[0]
        self.pc_ccf_offshore_l1y_ig = self.df['pc_ccf_offshore_l1y_ig'].values[0]
        self.pc_ccf_offshore_l1y_hy = self.df['pc_ccf_offshore_l1y_hy'].values[0]
        self.pc_ccf_offshore_m1y_ig = self.df['pc_ccf_offshore_m1y_ig'].values[0]
        self.pc_ccf_offshore_m1y_hy = self.df['pc_ccf_offshore_m1y_hy'].values[0]
        self.pc_mitg_ead_kreg = self.df['pc_mitg_ead_kreg'].values[0]
        self.pc_mitg_ead_cea = self.df['pc_mitg_ead_cea'].values[0]
        self.pc_ccf_roe_m1y_ig = self.df['pc_ccf_roe_m1y_ig'].values[0]
        self.pc_ccf_roe_l1y_ig = self.df['pc_ccf_roe_l1y_ig'].values[0]
        self.pc_ccf_roe_l1y_hy = self.df['pc_ccf_roe_l1y_hy'].values[0]
        self.pc_ccf_roe_m1y_hy = self.df['pc_ccf_roe_m1y_hy'].values[0]
        self.ds_veiculo_legal = self.df['ds_veiculo_legal'].values[0]
        self.modalidade = self.df['cd_modalidade'].values[0]      

    def get_ccf_l1y_ig_cea_off(self):
        return float(self.pc_ccf_onshore_l1y_ig)/100
    def get_ccf_m1y_ig_cea_off(self):
        return float(self.pc_ccf_offshore_m1y_ig)/100
    def get_ccf_m1y_hy_cea_off(self):
        return float(self.pc_ccf_offshore_m1y_hy)/100
    def get_ccf_l1y_hy_cea_off(self):
        return float(self.pc_ccf_offshore_l1y_hy)/100
    def get_ccf_l1y_ig_cea(self):
        return float(self.pc_ccf_onshore_l1y_ig)/100
    def get_ccf_m1y_ig_cea(self):
        return float(self.pc_ccf_onshore_m1y_ig)/100
    def get_ccf_l1y_hy_cea(self):
        return float(self.pc_ccf_onshore_l1y_hy)/100
    def get_ccf_m1y_hy_cea(self):
        return float(self.pc_ccf_onshore_m1y_hy)/100
    def get_ccf_l1y_ig_kreg(self):
        return float(self.pc_ccf_roe_l1y_ig)/100
    def get_ccf_m1y_ig_kreg(self):
        return float(self.pc_ccf_roe_m1y_ig)/100
    def get_ccf_l1y_hy_kreg(self):
        return float(self.pc_ccf_roe_l1y_hy)/100
    def get_ccf_m1y_hy_kreg(self):
        return float(self.pc_ccf_roe_m1y_hy)/100
    def get_mitigacaoead_cea(self):
        return float(self.pc_mitg_ead_cea)/100
    def get_mitigacaokreg(self):
        return float(self.pc_mitg_ead_kreg)/100
    def get_modalidade(self):
        return self.modalidade

    