# Imports obrigatorios
import unittest

#Imports das classes

from Objetos.Fluxo import Fluxo                         #Import da classe ObjFuxo
from Capital.Capital import *             
from ToolKitDB.CVA import *
from ToolKitDB.FEPF import * 

class testsUnitarios(unittest.TestCase):

    def test_fluxo1(self):     #Funcao responsavel pelo primeiro teste de fluxo
        fluxo = Fluxo(1,2,3,4,5,6,7,8)
        self.assertEqual(fluxo.get_data(), 1)
        self.assertEqual(fluxo.get_valor_desembolso(), 2)
        self.assertEqual(fluxo.get_valor_amortizacao(), 3)
        self.assertEqual(fluxo.get_pagamento_juros(), 4)
        self.assertEqual(fluxo.get_perc_mitigacao(), 5)
        self.assertEqual(fluxo.get_dc(), 6)
        self.assertEqual(fluxo.get_fpr(), 7)
        self.assertEqual(fluxo.get_spread(), 8)

    def test_fluxo2(self):    #Funcao responsavel pelo segundo teste de fluxo
        fluxo = Fluxo(1,2,3,4,5,6,7,8)
        self.assertEqual(fluxo.get_data(), 1)
        self.assertEqual(fluxo.get_valor_desembolso(), 2)
        self.assertEqual(fluxo.get_valor_amortizacao(), 3)
        self.assertEqual(fluxo.get_pagamento_juros(), 4)
        self.assertEqual(fluxo.get_perc_mitigacao(), 5)
        self.assertEqual(fluxo.get_dc(), 6)
        self.assertEqual(fluxo.get_fpr(), 7)
        self.assertEqual(fluxo.get_spread(), 8)

    def test_bba_rcp(self):     #funcao responsavel
        self.assertEqual(bba_rcp(3,4,0),4)
        self.assertAlmostEqual(bba_rcp(3,4,5),9.493061443340547,14)

    def test_cr_com_concentracao(self):
        self.assertEqual(cr_com_concentracao(9,8,7,6,5,4,3,2,1),270144)

    def test_defaut_pi(self):
        self.assertEqual(default_pi(10,20, 60), 0)

    def test_ajuste_prazo_bis(self):
        self.assertEqual(ajuste_prazo_bis(1500,15), 1.0028209874235892)    
     
    def test_calcular_kreg(self):
        self.assertEqual(calcular_kreg(25,5,4,1), 25)
        self.assertEqual(calcular_kreg(25,1,1,0), 0)
    
    def test_curva_hurdle(self):
        self.assertEqual(get_curva_hurdle(360), 0.125)
        self.assertEqual(get_curva_hurdle(730), 0.125)
        self.assertEqual(get_curva_hurdle(1095), 0.1264958904109589)
        self.assertEqual(get_curva_hurdle(1460), 0.1294917808219178)
        self.assertEqual(get_curva_hurdle(1825), 0.13448630136986303)
        self.assertEqual(get_curva_hurdle(2190), 0.13998493150684932)


if __name__ == '__main__':
    unittest.main(verbosity=2)