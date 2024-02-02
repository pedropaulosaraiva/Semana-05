import time
from typing import NamedTuple
import database.class_conn
import class_exception


##Classes de Dados
class dadosTrafoDist(NamedTuple):
    cod_id: str
    pac_1: str
    pac_2: str
    pac_3: str
    fas_con_p: str
    fas_con_s: str
    fas_con_t: str
    sit_ativ: str
    tip_unid: str
    ten_lin_se: str
    cap_elo: str
    cap_cha: str
    tap: str
    pot_nom: str
    per_fer: str
    per_tot: str
    ctmt: str
    tip_trafo: str
    uni_tr_s: str
    cod_id_eqtrd: str
    pac_1_eqtrd: str
    pac_2_eqtrd:str
    pac_3_eqtrd: str
    fas_con: str
    pot_nom_eqtrd: str
    lig: str
    ten_pri: str
    ten_sec: str
    ten_ter: str
    lig_fas_p: str
    lig_fas_s: str
    lig_fas_t: str
    per_fer_eqtrd: str
    per_tot_eqtrd: str
    r: str
    xhl: str

class dadosUnidCompReat(NamedTuple):
    cod_id: str
    fas_con: str
    pot_nom: str
    pac_1: str
    ctmt: str


class dadosSegLinhas(NamedTuple):
    cod_id: str
    ctmt: str
    pac_1: str
    pac_2: str
    fas_con: str
    comp: str
    tip_cnd: str
    uni_tr: str


class dadosUNREMT(NamedTuple):
    cod_id: str
    ctmt: str
    pac_1: str
    pac_2: str
    fas_con: str
    sit_ativ: str
    descr: str


class dadosUnidCons(NamedTuple):
    objectid: str
    pac: str
    ctmt: str
    fas_con: str
    ten_forn: str
    sit_ativ: str
    tip_cc: str
    car_inst: str
    ene_01: str
    ene_02: str
    ene_03: str
    ene_04: str
    ene_05: str
    ene_06: str
    ene_07: str
    ene_08: str
    ene_09: str
    ene_10: str
    ene_11: str
    ene_12: str
    uni_tr_s: str
    uni_tr: str

class dadosCondutores(NamedTuple):
    cod_id: str
    r1: str
    x1: str
    cnom: str
    cmax: str


class dadosCTATMT(NamedTuple):
    nome: str
    ten_nom: str
    cod_id: str


class dadosTransformador(NamedTuple):
    cod_id: str
    pac_1: str
    pac_2: str
    pot_nom: str
    lig: str
    ten_pri: str
    ten_sec: str
    ten_ter: str

class dadosTransformadorUNTRS(NamedTuple):
    cod_id: str
    pac_1: str
    barr_2: str


class dadosSECAT(NamedTuple):
    cod_id: str
    pac_1: str
    pac_2: str
    fas_con: str
    tip_unid: str
    p_n_ope: str
    cap_elo: str
    cor_nom: str
    sit_ativ: str


class dadosSECMT(NamedTuple):
    cod_id: str
    pac_1: str
    pac_2: str
    fas_con: str
    tip_unid: str
    ctmt: str
    uni_tr_s: str
    p_n_ope: str
    cap_elo: str
    cor_nom: str
    sit_ativ: str

class dadosALIMENTADOR(NamedTuple):
    ten_nom: str
    uni_tr_s: str
    nom: str
    cod_id: str
