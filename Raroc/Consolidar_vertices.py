from ToolKitDB.Vetor_operacao_df import *
from Objetos.Tela import Tela
from Objetos.Estoque_df import Estoque_df
from Objetos.Vertices_operacao import Vertice_operacao
from ToolKitFinance.ToolKit_finance import Toolkit_finance
from ToolKitDB.Moeda_conversao import Moeda_conversao
from ToolKitDB.Veiculo_legal import Veiculo_legal
from ToolKitDB.Visao_carteira import Visao_carteira
from ToolKitDB.Parametros import *
from ToolKitDB.Explodir_fluxo_vertices import *
from Capital.Capital import *
from ToolKitDB.Vetor_operacao_df import *
from Cluster.ConsolidarCluster import *
import math

def consolidar_vertices(vertices, toolkit_finance, moeda_conversao, tela, visao_carteira, veiculo_legal, array_constantes, k):
    
    #inicializacao de variaveis
    ead_kreg_pnl = 0
    ead_cea_pnl = 0
    cea_pnl = 0
    kreg_pnl = 0
    ir_cs_var = 0
    ir_cs_var_cap = 0
    mfb_pnl = 0
    receita_comissao_pnl = 0
    receita_termo_pnl = 0
    receita_rotativos_pnl = 0
    receita_fee_pnl = 0
    receita_adicional_pnl = 0
    remuneracao_cea = 0
    custo_n2_cea = 0
    remuneracao_kreg = 0
    custo_n2_kreg = 0
    despesas_tributarias_cea = 0
    despesas_tributaria_kreg = 0
    custo_full = 0
    lair_cea = 0
    lair_kreg = 0
    ircs_cea = 0
    ircs_kreg = 0
    pe_numerador = 0
    jcp_cea = 0
    jcp_kreg = 0
    rgo_cea = 0
    rgo_kreg = 0
    perc_cea = 0
    perc_kreg = 0
    cea_m = 0
    kreg_m = 0
    custo_capital = 0
    cv = 0
    raroc = 0
    roe = 0
    num_raroc_prosp = 0
    cea_prosp = 0
    num_roe_prosp = 0
    kreg_prosp = 0
    prazo_medio_prosp = 0
    ead_prosp = 0
    ead_cluster = 0
    cea_cluster = 0
    num_raroc_comb = 0
    cea_comb = 0
    num_roe_comb = 0
    kreg_comb = 0
    prazo_medio_comb = 0
    ead_comb = 0
    calculo_ead_cea_media = 0
    calculo_ead_kreg_media = 0
    calculo_prazo_total = 0
    calculo_ccf_cea = 0
    calculo_ccf_kreg = 0
    calculo_perc_uti = 0
    hurdle = 0
    amortizacao = 0
    prazo_acumulado = 0
    prazo_medio = 0
    lgd = 0
    ead = 0
    prz = 0
    pagamento_ativos = 0
    pagamento_passivos = 0
    pagamento_ativos_vp = 0
    pagamento_passivos_vp = 0
    pagamento_ativos_par = 0
    pagamento_passivos_par = 0
    resultado_venda = 0
    custo_bof = 0
    resultado_venda_liq = 0
    delta_cv = 0
    delta_pool = 0
    num_raroc_exp = 0
    raroc_carteira = 0
    roe_carteira_comb = 0
    raroc_carteira_comb = 0
    pagamento_juros_ativos = 0
    pagamento_juros_passivos = 0
    receita_12431 = 0
    receita_12431_vp = 0
    ir_passivos = 0;

    ir_ativos_normal = 0
    ir_ativos_12431 = 0
    ir_ativos_12431_vp = 0
    receitas = 0
    cea_vp = 0
    num_roe_exp = 0
    roe_carteira = 0
    vl_bof = 0
    
    #INFOS DE PRAZO VINDAS DA TELA    
    prazo_total = Tela.get_prazo_total(tela)
    if(Tela.get_prazo_quebrado(tela)):
        if(Tela.get_prazo_repac(tela)):
            prazo_final = int(Tela.get_prazo_final(tela))
            prazo_inicial = int(Tela.get_prazo_inicial(tela))
            calculo_prazo_total = f'{prazo_inicial} - {prazo_final} DC Considerando Repactuação'
            
        else:
            prazo_final = int(Tela.get_prazo_final(tela))
            prazo_inicial = int(Tela.get_prazo_inicial(tela))
            calculo_prazo_total = f'{prazo_inicial} - {prazo_final} DC'
            
    else:
            prazo_final = int(prazo_total)
            prazo_inicial = 0
            calculo_prazo_total = f'{Tela.get_prazo_total(tela)} Dias'
            
    prazo_decorrido = prazo_final - prazo_inicial      
    
    #PEGAR DO OBJETO VERTICE: PRAZO_DECORRIDO, EAD_CEA, DIAS_ATE_PROX_VERTICE, EAD_KREG, EAD_CLUSTER, CEA, KREG, MFB, PE, JUROS, PAGAMENTO
    array_info = []
    for i in range(len(vertices)):

        if(prazo_inicial <= vertices[i].prazo_decorrido <= prazo_final):
            if(vertices[i].prazo_decorrido == prazo_final):
                vertices[i].dias_ate_proximo_vertice = 0

            array_info.append([vertices[i].prazo_decorrido, vertices[i].dias_ate_proximo_vertice, vertices[i].ead_cea, vertices[i].ead_kreg, vertices[i].ead_cea_cluster, vertices[i].cea, vertices[i].kreg, vertices[i].mfb, vertices[i].pe, vertices[i].juros_acumulados_ativo, vertices[i].juros_acumulados_passivo, vertices[i].pagamento_juros_ativo, vertices[i].pagamento_juros_passivo, vertices[i].genesis, vertices[i].termination_vertex, vertices[i].indexador_ativo,  vertices[i].receita_comissao, vertices[i].receita_termo, vertices[i].receita_comprometida_rotativos])
            vertices_df = pd.DataFrame(array_info, columns=["prazo_decorrido", "dias_ate_proximo_vertice","ead_cea", "ead_kreg", "ead_cluster", "cea", "kreg", "mfb", "pe", "juros_ativo", "juros_passivo", "pagamento_ativo", "pagamento_passivo", "genesis_vertex", "termination_vertex", "indexador_ativo", "receita_comissao", "receita_termo", "receita_comprometida_rotativos"])

    vertices_np = vertices_df.values
    func_pv_dc = np.vectorize(Toolkit_finance.fator_pv_dc)
    func_forward = np.vectorize(Toolkit_finance.forward_curency_dc)
    func_curva_hurdle = np.vectorize(get_curva_hurdle)
    func_rem_cea = np.vectorize(Toolkit_finance.get_rem_cea_2)
    func_custo_n2 = np.vectorize(Toolkit_finance.get_custo_n2_2)
    func_accrual = np.vectorize(Toolkit_finance.get_accrual)
    
    #SEPARACAO DE VALORES DO VETOR PARA USO NO CALCULO
    array_prazo_decorrido = vertices_np[:,0]
    array_dias_ate_prox_vertice = vertices_np[:,1]
    array_ead_cea = vertices_np[:,2]
    array_ead_kreg = vertices_np[:,3]
    array_ead_cluster = vertices_np[:,4]
    array_cea = vertices_np[:,5]
    array_kreg = vertices_np[:,6]
    array_mfb = vertices_np[:,7]
    array_pe = vertices_np[:,8]
    array_juros_ativo = vertices_np[:,9]
    array_juros_passivo = vertices_np[:,10]
    array_pagamento_ativo = vertices_np[:,11]
    array_pagamento_passivo = vertices_np[:,12]
    array_genesis = vertices_np[:,13]
    array_termination = vertices_np[:,14]
    array_indexador_ativo = vertices_np[:,15]
    array_receita_comissao = vertices_np[:,16]
    array_receita_termo = vertices_np[:,17]
    array_receita_comprometidas = vertices_np[:,18]
    
    #FUNCOES VETORIZADAS
    if(k == 0):
        array_fator_pv_dc = func_pv_dc(toolkit_finance, Tela.get_indexador(tela), array_prazo_decorrido)
        array_forward_currency = func_forward(toolkit_finance,"BRL",array_indexador_ativo,array_prazo_decorrido)
        array_curva_hurdle = func_curva_hurdle(array_prazo_decorrido, Tela.get_raroc_hurdle(tela))
        array_accrual_venda = func_accrual(toolkit_finance,Tela.get_formato_venda(tela), "BRL", 0, array_prazo_decorrido, Tela.get_taxa_venda(tela), Tela.get_taxa_venda(tela))
        array_accrual_bof = func_accrual(toolkit_finance,Tela.get_formato_bof(tela), "BRL", 0,array_prazo_decorrido, Tela.get_taxa_bof(tela), Tela.get_taxa_bof(tela))
        array_accrual_cliente = func_accrual(toolkit_finance,Tela.get_formato_venda(tela), "BRL", 0, array_prazo_decorrido, Tela.get_taxa_cliente(tela), Tela.get_taxa_cliente(tela))
        array_accrual_ref = func_accrual(toolkit_finance,Tela.get_formato_bof(tela), "BRL", 0, array_prazo_decorrido, Tela.get_taxa_ref(tela), Tela.get_taxa_ref(tela))
        array_constantes = [array_fator_pv_dc, array_forward_currency, array_curva_hurdle, array_accrual_venda, array_accrual_bof, array_accrual_cliente, array_accrual_ref]
    else:
        array_fator_pv_dc = array_constantes[0]
        array_forward_currency = array_constantes[1]
        array_curva_hurdle = array_constantes[2]
        array_accrual_venda = array_constantes[3]
        array_accrual_bof = array_constantes[4]
        array_accrual_cliente = array_constantes[5]
        array_accrual_ref = array_constantes[6]
        
    #RODAR APENAS PARA OS QUE NAO SAO TERMINATION
    array_cea_aux = np.where(array_termination == False,(array_cea * array_dias_ate_prox_vertice * array_fator_pv_dc)/(Moeda_conversao.get_spot(moeda_conversao) * array_forward_currency),0)

    #ARRAYS DAS FUNCOES VETORIZADAS QUE UTILIZAM CEA_AUX
    array_prazo_ajustado = array_prazo_decorrido + array_dias_ate_prox_vertice
    array_rem_cea = func_rem_cea(toolkit_finance, array_cea_aux, array_prazo_ajustado, array_prazo_decorrido)
    array_custo_n2 = func_custo_n2(toolkit_finance, array_cea_aux, array_prazo_ajustado, array_prazo_decorrido)
#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ead_cea_pnl = (array_ead_cea * array_dias_ate_prox_vertice * array_fator_pv_dc).sum()
    ead_kreg_pnl = (array_ead_kreg * array_dias_ate_prox_vertice * array_fator_pv_dc).sum()
    ead_cluster = (array_ead_cluster * array_dias_ate_prox_vertice * array_fator_pv_dc).sum()
    prazo_acumulado = (array_ead_cea * array_dias_ate_prox_vertice *array_fator_pv_dc * array_prazo_decorrido)

    #RODAR APENAS PARA OS QUE NAO SAO TERMINATION
    cea_pnl = np.where(array_termination == False,((array_cea * array_dias_ate_prox_vertice * array_fator_pv_dc)/ (Moeda_conversao.get_spot(moeda_conversao) * array_forward_currency)),0).sum()
    cea_cluster = np.where(array_termination == False, ((array_ead_cluster * array_dias_ate_prox_vertice * array_fator_pv_dc)/(Moeda_conversao.get_spot(moeda_conversao) * array_forward_currency)),0).sum()
    hurdle_aux = np.where(array_termination == False, ((array_cea * array_fator_pv_dc)/(Moeda_conversao.get_spot(moeda_conversao) * array_forward_currency)),0)
    hurdle = (hurdle_aux * array_curva_hurdle).sum()
    cea_vp = np.where(array_termination == False, ((array_cea * array_fator_pv_dc)/(Moeda_conversao.get_spot(moeda_conversao) * array_forward_currency)),0).sum()
    
    #APENAS ONDE PRAZO DECORRIDO != PRAZO_FINAL E QUE NAO SEJA TERMINATION
    remuneracao_cea = np.where(((array_prazo_decorrido != prazo_final) & (array_termination == False)),(array_rem_cea + array_dias_ate_prox_vertice),0).sum()
    custo_n2_cea = np.where(((array_prazo_decorrido != prazo_final) & (array_termination == False)), (array_custo_n2 + array_dias_ate_prox_vertice),0).sum()

    #SAIDA DO IF PRAZO DECORRIDO != PRAZO_FINAL MAS DENTRO DO IF QUE NAO SAO TERMINATION
    giro = remuneracao_cea - custo_n2_cea
    kreg_pnl = (np.where(array_termination == False, (array_kreg * array_dias_ate_prox_vertice * array_fator_pv_dc)/(Moeda_conversao.get_spot(moeda_conversao) * array_forward_currency),0)).sum()

    #SAIDA DOS IFs
    mfb_pnl = (array_mfb * array_fator_pv_dc).sum()
    receita_comissao_pnl = (array_receita_comissao * array_fator_pv_dc).sum()
    receita_termo_pnl = (array_receita_termo * array_fator_pv_dc).sum()
    receita_rotativos_pnl = (array_receita_comprometidas * array_fator_pv_dc).sum()

    pe_nan = list(map(lambda x:math.isnan(x), array_pe))
    array_pe = np.where(pe_nan, 0, array_pe)
    pe_numerador = (array_pe * array_fator_pv_dc).sum()

    #IF NOT GENESIS
    pagamento_juros_ativos = np.where((array_genesis == False),(array_juros_ativo*(-1)),0).sum()
    pagamento_juros_passivos = np.where((array_genesis == False),array_juros_passivo,0).sum()
    array_ir_ativos_normal = np.where((array_genesis == False), array_juros_ativo - (array_juros_ativo * Tela.get_aliquota_pis_cofins(tela) * Veiculo_legal.get_aliquota_ir_cs(veiculo_legal,Tela.get_id_veiculo_legal(tela))),0)
    array_ir_ativos_12431 = np.where((array_genesis == False), array_juros_ativo - (array_juros_ativo * Tela.get_aliquota_pis_cofins(tela) * Veiculo_legal.get_aliquota_ir_cs(veiculo_legal,Tela.get_id_veiculo_legal(tela)) + array_pagamento_ativo*0.15),0)
    ir_ativos_12431_vp = np.where((array_genesis == False),(array_ir_ativos_12431 * array_fator_pv_dc),0).sum()
    receita_12431 = np.where((array_genesis == False),(array_ir_ativos_normal - array_ir_ativos_12431),0).sum()
    receita_12431_vp = np.where((array_genesis == False),((array_ir_ativos_normal - array_ir_ativos_12431) * array_fator_pv_dc),0).sum()
    ir_passivos = np.where((array_genesis == False),array_juros_passivo - (array_juros_passivo * Tela.get_aliquota_pis_cofins(tela)) *  Veiculo_legal.get_aliquota_ir_cs(veiculo_legal,Tela.get_id_veiculo_legal(tela)) * array_fator_pv_dc,0).sum()

    pagamentos_ativos = np.where((array_genesis == False),array_pagamento_ativo,0).sum()

    pagamento_passivos = np.where((array_genesis == False),array_pagamento_passivo,0).sum()
    pagamento_ativos_vp = np.where((array_genesis == False),(array_pagamento_ativo/array_accrual_venda),0).sum()
    pagamento_passivos_vp = np.where((array_genesis == False),(array_pagamento_passivo/array_accrual_bof),0).sum()
    pagamento_ativos_par = np.where((array_genesis == False),(array_pagamento_ativo/array_accrual_cliente),0).sum()
    pagamento_passivos_par = np.where((array_genesis == False),(array_pagamento_passivo/array_accrual_ref),0).sum()

    #IF GENESIS
    receita_fee_pnl = np.where(array_genesis == True,Tela.get_receita_fee(tela),0).sum()
    
    #SAIDA DO IF
    receitas += mfb_pnl + receita_comissao_pnl + receita_termo_pnl + receita_fee_pnl + receita_rotativos_pnl
    ir_cs_var += (mfb_pnl + receita_comissao_pnl + receita_termo_pnl + receita_fee_pnl + receita_rotativos_pnl) * Veiculo_legal.get_aliquota_ir_cs(veiculo_legal, Tela.get_id_veiculo_legal(tela))
    ir_cs_var_cap += (mfb_pnl + receita_comissao_pnl + receita_termo_pnl + receita_fee_pnl + receita_rotativos_pnl) * Veiculo_legal.get_aliquota_ir_cs(veiculo_legal, "1222")

    aliquota_ajustada = ir_cs_var / receitas
    aliquota_ajustada_cap = ir_cs_var_cap / receitas

    if(not(aliquota_ajustada) > 0):
        aliquota_ajustada = Veiculo_legal.get_aliquota_ir_cs(veiculo_legal, Tela.get_id_veiculo_legal(tela))
    if(not(aliquota_ajustada_cap) > 0):
        aliquota_ajustada_cap = Veiculo_legal.get_aliquota_ir_cs(veiculo_legal, Tela.get_id_veiculo_legal(tela))
    
    cea_pnl += max((mfb_pnl + receita_termo_pnl + receita_rotativos_pnl + receita_fee_pnl + receita_comissao_pnl) * get_cea_ro(), 0) * (1 - Tela.get_aliquota_pis_cofins(tela)) * 30
    aux = max((mfb_pnl + receita_termo_pnl + receita_rotativos_pnl + receita_fee_pnl + receita_comissao_pnl) * get_cea_ro(), 0)
    hurdle = (hurdle / cea_vp)
    
    if(not(hurdle >= 0)):
        hurdle = get_curva_hurdle(prazo_final, Tela.get_raroc_hurdle(tela))
        
    prazo_medio = Vertice_operacao.obter_duration(vertices[0])
    lgd = Tela.get_lgd(tela) * (1 - Tela.get_mitigacao_cea(tela))

    remuneracao_kreg = Toolkit_finance.get_rem_cea(toolkit_finance, kreg_pnl, prazo_final - prazo_inicial)

    custo_n2_kreg = Toolkit_finance.get_custo_n2(toolkit_finance, kreg_pnl, prazo_final - prazo_inicial)

    if(Tela.get_onshore(tela)):
        teste = Tela.get_aliquota_iss(tela)
        despesas_tributarias_cea = (mfb_pnl + receita_termo_pnl + receita_fee_pnl + receita_comissao_pnl + (remuneracao_cea - custo_n2_cea)) * Tela.get_aliquota_pis_cofins(tela) + (receita_comissao_pnl) * Tela.get_aliquota_iss(tela)
        despesas_tributaria_kreg = (mfb_pnl + receita_termo_pnl + receita_fee_pnl + receita_comissao_pnl + (remuneracao_kreg - custo_n2_kreg)) * Tela.get_aliquota_pis_cofins(tela) + (receita_comissao_pnl) * Tela.get_aliquota_iss(tela)

        pis_mfb = mfb_pnl * Tela.get_aliquota_pis_cofins(tela)
    else:
        despesas_tributarias_cea = (remuneracao_cea - custo_n2_cea) * Tela.get_aliquota_pis_cofins(tela)

        despesas_tributaria_kreg = (remuneracao_kreg - custo_n2_kreg) * Tela.get_aliquota_pis_cofins(tela)
        
    if(Tela.get_onshore(tela)):
        cof = Tela.get_aliquota_pis_cofins(tela)
        custo_full = Tela.get_ie(tela) * ((mfb_pnl + receita_termo_pnl + receita_rotativos_pnl + receita_fee_pnl + receita_comissao_pnl) - ((mfb_pnl + receita_termo_pnl + receita_rotativos_pnl + receita_fee_pnl + receita_comissao_pnl) * Tela.get_aliquota_pis_cofins(tela) + (receita_comissao_pnl) * (Tela.get_aliquota_iss(tela)))) + Tela.get_custo_manual(tela) 
        custo_full = abs(custo_full)
        custo_full = min(custo_full, (1000000 + (((prazo_final - prazo_inicial) / 365) * 1000000)) / Moeda_conversao.get_spot(moeda_conversao))
    else:
        custo_full = Tela.get_ie(tela) * ((mfb_pnl + receita_termo_pnl + receita_rotativos_pnl) + (receita_comissao_pnl + receita_fee_pnl) * 1) + Tela.get_custo_manual(tela)
        custo_full = abs(custo_full)
        conversao = Moeda_conversao.get_spot(moeda_conversao)
        custo_full = min(custo_full, (1000000 + (((prazo_final - prazo_inicial) / 365) * 1000000)) / Moeda_conversao.get_spot(moeda_conversao))


    receita_12431_vp = receita_12431_vp * (1 - Tela.get_ie(tela))

    vl_bof = Tela.get_perc_bof(tela) * ((prazo_final - prazo_inicial)/365) * (ead_cea_pnl/prazo_total)

    lair_cea = (mfb_pnl + receita_comissao_pnl + receita_termo_pnl + receita_fee_pnl + receita_rotativos_pnl + remuneracao_cea - custo_n2_cea - despesas_tributarias_cea - custo_full - vl_bof - pe_numerador)
    lair_kreg = (mfb_pnl + receita_comissao_pnl + receita_termo_pnl + receita_rotativos_pnl + receita_fee_pnl + remuneracao_kreg - custo_n2_kreg - despesas_tributaria_kreg - custo_full - pe_numerador)

    if(Tela.get_modalidade(tela) != "12431"):
        ircs_cea = (aliquota_ajustada * (lair_cea - (remuneracao_cea - custo_n2_cea)) + (remuneracao_cea - custo_n2_cea) * aliquota_ajustada_cap)
        ircs_kreg = Tela.get_ircs(tela) * lair_kreg
    elif(Tela.get_formato_taxa(tela) == "IPCA" or Tela.get_formato_taxa(tela) == "EXP/252M" or Tela.get_formato_taxa(tela) == "FLAT" or Tela.get_formato_taxa(tela) == "%CDI"):
        ircs_cea = (aliquota_ajustada * ((lair_cea + pe_numerador) - (remuneracao_cea - custo_n2_cea)) + (remuneracao_cea - custo_n2_cea) * aliquota_ajustada_cap - pe_numerador * 0.20)
        ircs_kreg = Tela.get_ircs(tela) * lair_kreg
    else:
        ircs_cea = (aliquota_ajustada * ((lair_cea + pe_numerador) - (remuneracao_cea - custo_n2_cea)) + (remuneracao_cea - custo_n2_cea) * aliquota_ajustada_cap - pe_numerador * 0.20)
        ircs_kreg = Tela.get_ircs(tela) * lair_kreg
        
    jcp_cea = Toolkit_finance.get_jcp(toolkit_finance,cea_pnl, prazo_final - prazo_inicial)
    jcp_kreg = Toolkit_finance.get_jcp(toolkit_finance, kreg_pnl, prazo_final - prazo_inicial)

    rgo_cea = lair_cea - ircs_cea + jcp_cea
    
    rgo_kreg = lair_kreg - ircs_kreg + jcp_kreg

    cea_pnl = ((pow(1 + hurdle, (1 / 360)) - 1) / (pow(1 + get_curva_hurdle(1, Tela.get_raroc_hurdle(tela)), (1 / 360)) - 1)) * cea_pnl
    
    raroc = pow(1 + (rgo_cea / cea_pnl), 360) - 1          

    roe = pow(1 + (rgo_kreg / kreg_pnl), 360) - 1          

    delta_hurdle = hurdle - get_curva_hurdle(1, Tela.get_raroc_hurdle(tela))

    cea_m = cea_pnl / (prazo_final - prazo_inicial)
    
    if(math.isnan(cea_m)):
        cea_m = 0

    kreg_m = kreg_pnl / (prazo_final - prazo_inicial)

    perc_cea = cea_pnl / ead_cea_pnl
    
    if(math.isnan(perc_cea)):
        perc_cea = 0

    perc_kreg = kreg_pnl / ead_kreg_pnl

    custo_capital = (pow((1 + Tela.get_raroc_hurdle(tela)), 1/360) - 1) * cea_pnl
    
    if(math.isnan(custo_capital)):
        custo_capital = 0

    cv = rgo_cea - custo_capital
    
    if(math.isnan(cv)):
        cv = 0
    
    resultado_venda = (pagamento_ativos_vp - pagamento_ativos_par) - (pagamento_passivos_vp - pagamento_passivos_par)
    resultado_venda_liq = resultado_venda * 0.55
    delta_cv = resultado_venda_liq - cv
    delta_pool = 2 * resultado_venda_liq - (rgo_cea + 2*cv)

    prazo_medio_prosp = Visao_carteira.get_prazo_medio(visao_carteira) * 30
    num_raroc_prosp = Visao_carteira.get_num_raroc(visao_carteira)
    num_roe_prosp = Visao_carteira.get_num_roe(visao_carteira)

    if(num_raroc_prosp != 0):
        kreg_prosp = (Visao_carteira.get_cr_vp(visao_carteira) / prazo_medio_prosp) * 30
        
        if(math.isnan(kreg_prosp)):
            kreg_prosp = 0
        
        ead_prosp = Visao_carteira.get_ead_vp(visao_carteira)
        
        cea_prosp = (Visao_carteira.get_cea_vp(visao_carteira) / prazo_medio_prosp) * 30
        
        if(math.isnan(cea_prosp)):
            cea_prosp = 0
        
        num_raroc_exp = (((pow(((360 * num_raroc_prosp) + (cea_prosp * prazo_medio_prosp)), (1 / 360))) / pow((cea_prosp * prazo_medio_prosp), (1 / 360))) - 1) * (cea_prosp * prazo_medio_prosp)
        
        if(math.isnan(num_raroc_exp)):
            num_raroc_exp = 0
        
        raroc_carteira = pow((1 + (num_raroc_exp/ (cea_prosp * prazo_medio_prosp))), 360) - 1
        
        if(math.isnan(raroc_carteira)):
            raroc_carteira = 0
        
        num_roe_exp = (((pow(((360 * num_roe_prosp) + (kreg_prosp * prazo_medio_prosp)), (1 / 360))) / pow((kreg_prosp * prazo_medio_prosp), (1 / 360))) - 1) * (kreg_prosp * prazo_medio_prosp)
        
        if(math.isnan(num_roe_exp)):
            num_roe_exp = 0

        roe_carteira = pow((1 + (num_roe_exp / (kreg_prosp * prazo_medio_prosp))), 360) - 1
    
    else:
        kreg_prosp = 0
        cea_prosp = 0
        num_raroc_exp = 0
        raroc_carteira = 0
        num_roe_exp = 0
        roe_carteira = 0 

    prazo_medio_comb = prazo_medio_prosp
    num_raroc_comb = num_raroc_exp + rgo_cea * Moeda_conversao.get_spot(moeda_conversao)
    num_roe_comb = num_roe_exp + rgo_kreg * Moeda_conversao.get_spot(moeda_conversao)
    kreg_comb = ((kreg_pnl * Moeda_conversao.get_spot(moeda_conversao)) + (kreg_prosp * prazo_medio_prosp)) / prazo_medio_comb
    
    cea_comb = ((cea_pnl * Moeda_conversao.get_spot(moeda_conversao)) + (cea_prosp * prazo_medio_prosp)) / prazo_medio_comb
    
    roe_carteira_comb = pow(1 + (num_roe_comb / (kreg_comb * prazo_medio_comb)), 360) - 1

    raroc_carteira_comb = pow(1 + (num_raroc_comb / (cea_comb * prazo_medio_comb)), 360) - 1
    
    if(math.isnan(raroc_carteira_comb)):
        raroc_carteira_comb = 0
    
    mfb_pnl += receita_comissao_pnl
    receita_adicional_pnl = receita_termo_pnl + receita_rotativos_pnl + receita_fee_pnl    

    ########################################
    giro_raroc = remuneracao_cea - custo_n2_cea

    if(math.isnan(giro_raroc)):
        giro_raroc = 0
    
    if(math.isnan(ircs_cea)):
        ircs_cea = 0
    
    if(math.isnan(jcp_cea)):
        jcp_cea = 0
    
    if(math.isnan(pe_numerador)):
        pe_numerador = 0
    
    if(math.isnan(raroc)):
        raroc = 0
    
    if(math.isnan(lair_cea)):
        lair_cea = 0
    
    if(math.isnan(rgo_cea)):
        rgo_cea = 0

    if(math.isnan(receita_adicional_pnl)):
        receita_adicional_pnl = 0
    
    if(math.isnan(despesas_tributarias_cea)):
        despesas_tributarias_cea = 0
    
    if(math.isnan(delta_pool)):
        delta_pool = 0
    
    if(math.isnan(delta_cv)):
        delta_cv = 0
    
    if(math.isnan(ircs_kreg)):
        ircs_kreg = 0
    
    if(math.isnan(lair_kreg)):
        lair_kreg = 0
    
    if(math.isnan(rgo_kreg)):
        rgo_kreg = 0
    

   
    consolidado = {
        "EAD_PNL": ead_cea_pnl,
        "CALCULO_SPREAD_RAROC": Tela.get_spread(tela),    
        "CALCULO_SPREAD_ROE": Tela.get_spread(tela), 
        "CALCULO_MFB_RAROC": round(mfb_pnl), 
        "calculo_mfb_roe": round(mfb_pnl), 
        "CALCULO_RECEITAS_RAROC": round(receita_adicional_pnl), 
        "calculo_receitas_roe": round(receita_adicional_pnl), 
        "CALCULO_GIROPROPRIO_RAROC": round(giro_raroc),
        "calculo_giroproprio_roe": round(remuneracao_kreg - custo_n2_kreg),
        "CALCULO_TRIBUTARIAS_RAROC": round(despesas_tributarias_cea),
        "calculo_tributarias_roe": round(despesas_tributaria_kreg), 
        "CALCULO_PERDA_RAROC": round(pe_numerador), 
        "calculo_perda_roe": round(pe_numerador),
        "CALCULO_CUSTOSADM_RAROC": round(custo_full),
        "calculo_custo_adm_roe": round(custo_full),
        "CALCULO_RESULTADO_ANTES_IR_RAROC": round(lair_cea),
        "calculo_resultado_antes_ir_roe": round(lair_kreg),
        "CALCULO_IR_RAROC": round(ircs_cea),
        "calculo_ir_roe": round(ircs_kreg),
        "CALCULO_JCP_RAROC": round(jcp_cea),
        "calculo_jcp_roe": round(jcp_kreg),
        "CALCULO_RGO_RAROC": round(rgo_cea),
        "calculo_rgo_roe": round(rgo_kreg),
        "CALCULO_CEA_RAROC": round(cea_m),
        "calculo_kreg_roe": round(kreg_m),
        "CALCULO_PERC_CEA_RAROC": perc_cea,        
        "calculo_perc_kreg_roe": perc_kreg,
        "CALCULO_CUSTO_CAPITAL": round(custo_capital),
        "CALCULO_CRIACAO_VALOR": round(cv),
        "CALCULO_RAROC_OPERACAO": raroc,
        "CALCULO_RAROC_GRUPO": raroc_carteira,
        "CALCULO_RAROC_COMBINADO": raroc_carteira_comb,
        "CALCULO_EAD_CEA_MEDIA": round(ead_cea_pnl/prazo_decorrido),
        "CALCULO_EAD_KREG_MEDIA": round(ead_kreg_pnl/prazo_decorrido),
        "CALCULO_PZO_TOTAL": calculo_prazo_total,
        "CALCULO_CCF_CEA": (calculo_ccf_cea * 100),
        "CALCULO_CCF_KREG": (calculo_ccf_kreg * 100),
        "CALCULO_PERC_UTI": "Em breve",
        "HURDLE": hurdle,
        "VL_BOF": round(vl_bof),
        "PGTO_ATIVOS": pagamento_ativos,
        "PGTO_PASSIVOS": pagamento_passivos,
        "PGTO_ATIVOS_VP": pagamento_ativos_vp, 
        "PGTO_PASSIVOS_VP": pagamento_passivos_vp, 
        "PGTO_ATIVOS_PAR": pagamento_ativos_par,
        "PGTO_PASSIVOS_PAR": pagamento_passivos_par,
        "RESULTADO_VENDA": resultado_venda,
        "RESULTADO_VENDA_LIQ": resultado_venda_liq,
        "DELTA_CV": (delta_cv),
        "DELTA_POOL": (delta_pool)
        }

    return [consolidado, array_constantes]        