class Fluxo:    
    def __init__(self, data, desembolso, amortizacao, pagamento_juros, 
                 perc_mitigacao, dc, fpr, spread):

        self.data = data
        self.valor_desembolso = desembolso
        self.valor_amortizacao = amortizacao
        self.pagamento_juros = pagamento_juros
        self.perc_mitigacao = perc_mitigacao
        self.dc = dc
        self.fpr = fpr
        self.spread = spread

    def get_data(self):
        return self.data

    def get_valor_desembolso(self):
        return float(self.valor_desembolso)
            
    def get_valor_amortizacao(self):
        return float(self.valor_amortizacao)

    def get_pagamento_juros(self):
        if(self.pagamento_juros == "false" or self.pagamento_juros == "False" or self.pagamento_juros == False):
            self.pagamento_juros = False
        if(self.pagamento_juros == "true" or self.pagamento_juros == "True" or self.pagamento_juros == True):
            self.pagamento_juros = True
        
        return self.pagamento_juros

    def get_perc_mitigacao(self):
        return float(self.perc_mitigacao)

    def get_dc(self):
        return self.dc

    def get_fpr(self):
        return self.fpr

    def get_spread(self):
        return self.spread

