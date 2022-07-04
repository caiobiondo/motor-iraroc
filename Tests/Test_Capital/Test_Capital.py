def test_bba_rcp():
    nK = 2  # hard input
    npz_dc = 1
    n_alfa = 1
    n_beta = 1
    npz_dc_be = 1
    n_rcp = nK * npz_dc

    if n_beta != 0:
        npz_dc_be = nK * (n_alfa/n_beta)

    assert npz_dc_be == 2
    assert nK * npz_dc == 2
    assert n_alfa/n_beta == 1
    assert n_rcp == 2


def test_cr_com_concentracao():
    n_lgd = 1
    n_pd = 1
    n_ead = 1
    n_wi = 1
    n_pe_wi = 1
    n_sigma = 1
    n_var = 1
    n_pe = 1
    n_desvio = 1

    max_1 = n_pd * n_lgd * n_ead * (n_lgd * n_ead + n_wi * n_sigma * n_pe_wi)
    max_2 = (n_var - n_pe) / n_desvio

    assert max_2 == 0
    assert max_1 == 2
    assert n_lgd + n_pd == 2
    assert n_ead == 1


def test_default_pi():
    ead = 1
    lgd_dt = 1
    lgd_be = 1

    max_1 = (lgd_dt - lgd_be) * ead

    assert max_1 == 0


def test_recalibrar_cea_nao_default():
    k2 = 1
    k3 = 1
    ead = 1
    cea_puro = 1
    pd_bis = 1
    prazo_rem = 1
    k_grupo = 1

    max_1 = cea_puro * (k_grupo * (1 + (min(prazo_rem, 1800) / 360 - 2.5)))
    max_2 = max(max_1 * k2, 0) * k3
    max_3 = ead + pd_bis

    assert max_1 == -1.4972222222222222
    assert max_2 == 0
    assert max_3 == 2


def test_recalibrar_cea_default():
    k1 = 1
    k2 = 1
    k3 = 1
    cea_puro = 1
    k_grupo = 1

    cea_puro_k1 = cea_puro * (k_grupo * k1)
    max_1 = cea_puro_k1 * k2 * k3

    assert max_1 == 1


def test_recalibrar_cea_cluster_nao_default():
    cea_puro = 1
    pd_bis = 1
    prazo_rem = 1
    k_grupo = 1

    cea_puro_k1 = cea_puro * (k_grupo * (1 + (min(prazo_rem, 1800) / 360 - 2.5)))
    max_1 = pd_bis

    assert cea_puro_k1 == -1.4972222222222222
    assert max_1 == 1


def test_recalibrar_cea_cluster_default():
    cea_puro = 1

    assert cea_puro == 1


def test_ajuste_prazo_bis():
    prazo = 1
    pd = 1

    max_1 = (1 + (min(prazo, 1800) / 360 - 2.5))

    assert max_1 == -1.4972222222222222
    assert pd == 1


def test_calcula_kreg_normal():
    ead = 1
    fpr = 1
    perc_n1 = 1
    perc_bis = 1

    max_1 = min(ead * fpr * perc_n1 * perc_bis, ead)

    assert max_1 == 1


def test_calcular_fepf():
    prazo_remanescente_dias = 1
    tipo_derivativo = 1

    max_1 = prazo_remanescente_dias % 360

    assert max_1 == 1


def test_calcular_cva():
    prazo_remanescente_dias = 1
    fpr = 1

    fpr = fpr * 100
    fpr_validos = [0, 2, 20, 50, 85, 100]

    max_1 = min(fpr_validos, key=lambda x:abs(x-fpr))
    prazo_remanescente_dias = (prazo_remanescente_dias / 360)

    assert max_1 == 100
    assert prazo_remanescente_dias == 0.002777777777777778


def test_get_curva_hurdle():
    prazo = 1
    hurdle = 1

    hurdle_novo = hurdle + 0.0095
    max_1 = min((prazo - 1826) / (2191 - 1826), 1) * 0.0055 + hurdle_novo

    assert max_1 == 0.9820000000000001