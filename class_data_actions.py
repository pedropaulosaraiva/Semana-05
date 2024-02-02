import time

import database.class_data as data
import database.class_conn
import class_exception
from database.class_data import dadosCTATMT


class C_DBaseData():

    def __init__(self):
        super(C_DBaseData, self).__init__()
        self._DataBaseConn = database.class_conn.C_DBaseConn()  # Criando a instância do Banco de Dados

    @property
    def DataBaseConn(self):
        return self._DataBaseConn

    @DataBaseConn.setter
    def DataBaseConn(self, value):
        self._DataBaseConn = value

    def getData_EqThevenin(self, nomeCircuitoAlta):  # OBS: Método obsoleto
        """
        Seleciona colunas do banco CTAT de um determinado circuito e as armazena em uma lista


        :param nomeCircuitoAlta: str, nome do circuito de alta tensão escolhido


        :return lista_alimentador_SE: list[NamedTuple], lista dos dados do circuito escolhido
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = "SELECT nom, ten_nom, cod_id FROM ctat WHERE nom ='" + nomeCircuitoAlta + "'"
                case "2021":
                    sqlQuery = "SELECT nome, ten_nom, cod_id FROM ctat WHERE nom ='" + nomeCircuitoAlta + "'"

            alimentadores_SE = self.DataBaseConn.getSQLDB("CTAT", sqlQuery)

            lista_alimentador_SE: list[dadosCTATMT] = []
            for alimentador_SE in alimentadores_SE.fetchall():
                tmp_dados = data.dadosCTATMT(
                    alimentador_SE[0],  # nome: str
                    alimentador_SE[1],  # ten_nom: str
                    alimentador_SE[2],  # cod_id: str
                )

                lista_alimentador_SE.append(tmp_dados)

            return lista_alimentador_SE

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para o Equivalente de Thevenin!")

    def getData_CTMT_EQTH(self, pac_2=None):  # OBS: Por que o if pac_2?
        """
        Seleciona colunas do banco CTMT de um determinado pac e as armazena em uma lista


        :param pac_2: str, ponto de aclopamento comum do circuito CTMT escolhido


        :return lista_CTMTs_pac: list[NamedTuple], lista dos dados do circuito escolhido
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    if pac_2 is not None:
                        sqlQuery = "SELECT ten_nom, uni_tr_s, nom, cod_id FROM ctmt WHERE pac ='" + pac_2
                case "2021":
                    if pac_2 is not None:
                        sqlQuery = "SELECT ten_nom, uni_tr_at, nome, cod_id FROM ctmt WHERE pac_ini ='" + pac_2

            CTMTs_pac = self.DataBaseConn.getSQLDB("CTMT", sqlQuery)

            lista_CTMT_pac = []
            for CTMT_pac in CTMTs_pac.fetchall():
                tmp_dados = data.dadosALIMENTADOR(
                    CTMT_pac[0],  # ten_nom: str
                    CTMT_pac[1],  # uni_tr_s: str
                    CTMT_pac[2],  # nom: str
                    CTMT_pac[3],  # cod_id str

                )
                lista_CTMT_pac.append(tmp_dados)

            return lista_CTMT_pac

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para o Equivalente de Thevenin" +
                                              " de Média Tensão!")

    def getData_CTMT(self, nomeSE_MT=None):
        """
        Seleciona colunas do banco CTMT de uma determinada sub, se determinada, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subesteação escolhida


        :return lista_CTMTs_pac: list[NamedTuple], lista dos dados dos circuito da subestação
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = "SELECT nom, ten_nom, cod_id  FROM ctmt ORDER BY cod_id"
                case "2021":
                    sqlQuery = "SELECT nome, ten_nom, cod_id  FROM ctmt ORDER BY cod_id"

            if nomeSE_MT is not None:
                sqlQuery += " WHERE sub ='" + nomeSE_MT + "'"

            CTMTs_sub = self.DataBaseConn.getSQLDB("CTMT", sqlQuery)

            lista_CTMTs_sub = []
            for CTMT_sub in CTMTs_sub.fetchall():
                tmp_dados = data.dadosCTATMT(
                    CTMT_sub[0],  # nome: str
                    CTMT_sub[1],  # ten_nom: str
                    CTMT_sub[2],  # cod_id: str

                )

                lista_CTMTs_sub.append(tmp_dados)

            return lista_CTMTs_sub

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para o Equivalente de Thevenin" +
                                              " de Média Tensão!")

    def getData_TrafosAT_MT(self, nomeSE_MT):
        """
        Seleciona colunas dos bancos de TRs_AT de uma determinada sub, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subesteação escolhida


        :return lista_EQTRATs_sub: list[NamedTuple], lista dos dados dos transformadores de alta tensão da subestação
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = "SELECT cod_id, pac_1, pac_2, pot_nom, lig, ten_pri, ten_sec, ten_ter FROM eqtrs \
                                WHERE pac_1 LIKE '" + nomeSE_MT + "%'"
                    EQTRATs_sub = self.DataBaseConn.getSQLDB("EQTRS", sqlQuery)

                    lista_EQTRATs_sub = []
                    for EQTRAT_sub in EQTRATs_sub.fetchall():
                        tmp_dados = data.dadosTransformador(
                            EQTRAT_sub[0],  # cod_id
                            EQTRAT_sub[1],  # pac_1
                            EQTRAT_sub[2],  # pac_2
                            EQTRAT_sub[3],  # pot_nom
                            EQTRAT_sub[4],  # lig
                            EQTRAT_sub[5],  # ten_pri
                            EQTRAT_sub[6],  # ten_sec
                            EQTRAT_sub[7],  # ten_ter
                        )
                        lista_EQTRATs_sub.append(tmp_dados)

                case "2021":
                    sqlQuery = "SELECT cod_id, pot_nom, lig, ten_pri, ten_sec, ten_ter FROM eqtrat \
                                WHERE UNI_TR_AT LIKE '" + nomeSE_MT + "%'"
                    EQTRATs_sub = self.DataBaseConn.getSQLDB("EQTRAT", sqlQuery)

                    sqlQuery_pac = "SELECT pac_1, pac_2 FROM untrat WHERE pac_1 LIKE '" + nomeSE_MT + "%'"
                    EQTRATs_sub_pac = self.DataBaseConn.getSQLDB("UNTRAT", sqlQuery_pac)

                    lista_EQTRATs_sub = []
                    for EQTRAT_sub, EQTRAT_sub_pac in zip(EQTRATs_sub.fetchall(), EQTRATs_sub_pac.fetchall()):
                        tmp_dados = data.dadosTransformador(
                            EQTRAT_sub[0],  # cod_id
                            EQTRAT_sub_pac[0],  # pac_1
                            EQTRAT_sub_pac[1],  # pac_2
                            EQTRAT_sub[1],  # pot_nom
                            EQTRAT_sub[2],  # lig
                            EQTRAT_sub[3],  # ten_pri
                            EQTRAT_sub[4],  # ten_sec
                            EQTRAT_sub[5],  # ten_ter
                        )
                        lista_EQTRATs_sub.append(tmp_dados)

            return lista_EQTRATs_sub

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para os Transformadores " +
                                              "de Média Tensão!")

    def getData_TRAFO_UNTRS(self, uni_tr_at):
        """
        Seleciona colunas do banco de TRs_AT de uma determinada UNTRAT, e as armazena em uma lista


        :param uni_tr_at: str, código da unidade transformadora de alta tensão escolhida


        :return lista_UNTRAT: list[NamedTuple], lista dos dados da unidade transformadora de alta tensão
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = "SELECT cod_id, pac_1,barr_2 FROM untrs WHERE cod_id='" + uni_tr_at + "'"
                    UNTRAT_cod = self.DataBaseConn.getSQLDB("UNTRS", sqlQuery)
                case "2021":
                    sqlQuery = "SELECT cod_id, pac_1,barr_2 FROM untrat WHERE cod_id='" + uni_tr_at + "'"
                    UNTRAT_cod = self.DataBaseConn.getSQLDB("UNTRAT", sqlQuery)

            lista_UNTRAT = []
            for UNTRAT in UNTRAT_cod.fetchall():
                tmp_dados = data.dadosTransformadorUNTRS(
                    UNTRAT[0],  # cod_id
                    UNTRAT[1],  # pac_1
                    UNTRAT[2],  # barr_2
                )
                lista_UNTRAT.append(tmp_dados)

            return lista_UNTRAT

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para os Transformadores " +
                                              "de Média Tensão!")

    def getData_Condutores(self, tipoCondutor):
        """
        Seleciona colunas do banco de SEGCOM de um determinado tipo (B, M ou R), e as armazena em uma lista


        :param tipoCondutor: str, tipo de condutores: B = baixa tensão, M = média tensão e R = ramal


        :return lista_SEGCONs: list[NamedTuple], lista dos dados dos segmentos selecionados
        """
        start_time = time.time()
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT cod_id, r1, x1, cnom, cmax FROM segcon WHERE cod_id LIKE '%" + tipoCondutor +
                                "%' ORDER BY cod_id")
                case "2021":
                    sqlQuery = ("SELECT cod_id, r1, x1, cnom, cmax FROM segcon WHERE cod_id LIKE '%" + tipoCondutor +
                                "%' ORDER BY cod_id")

            SEGCONs = self.DataBaseConn.getSQLDB("SEGCON", sqlQuery)

            lista_SEGCONs = []
            for SEGCON in SEGCONs.fetchall():
                tmp_dados = data.dadosCondutores(
                    SEGCON[0],  # cod_id: str
                    SEGCON[1],  # r1: str
                    SEGCON[2],  # x1: str
                    SEGCON[3],  # cnom: str
                    SEGCON[4],  # cmax: str
                )
                lista_SEGCONs.append(tmp_dados)

            print("--- %s seconds --- CONDUTORES" % (time.time() - start_time))
            return lista_SEGCONs

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para os Condutores: " +
                                              tipoCondutor)

    def getData_SecAT(self, nomeSE_MT):
        """
        Seleciona colunas do banco de UNSEAT de uma determinada sub, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subestação


        :return lista_UNSEATs_sub: list[NamedTuple], lista dos dados das unidades seccionadoras da subestação
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT cod_id, pac_1, pac_2, fas_con, tip_unid, p_n_ope, cap_elo, cor_nom, sit_ativ " +
                                "FROM unseat WHERE sub = '" + nomeSE_MT + "'")

                case "2021":
                    sqlQuery = ("SELECT cod_id, pac_1, pac_2, fas_con, tip_unid, p_n_ope, cap_elo, cor_nom, sit_ativ " +
                                "FROM unseat WHERE sub = '" + nomeSE_MT + "'")

            UNSEATs_sub = self.DataBaseConn.getSQLDB("UNSEAT", sqlQuery)

            lista_UNSEATs_sub = []
            for UNSEAT_sub in UNSEATs_sub.fetchall():
                tmp_dados = data.dadosSECAT(
                    UNSEAT_sub[0],  # cod_id
                    UNSEAT_sub[1],  # pac_1
                    UNSEAT_sub[2],  # pac_2
                    UNSEAT_sub[3],  # fas_con
                    UNSEAT_sub[4],  # tip_unid
                    UNSEAT_sub[5],  # p_n_ope
                    UNSEAT_sub[6],  # cap_elo
                    UNSEAT_sub[7],  # cor_nom
                    UNSEAT_sub[8],  # sit_ativ
                )
                lista_UNSEATs_sub.append(tmp_dados)

            return lista_UNSEATs_sub

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para as Seccionadoras de AT")

    def getData_SecMT(self, nomeSE_MT, tipoSEC):
        """
        Seleciona colunas do banco de UNSEMT de uma determinada sub e tipo, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subestação
        :param tipoSEC: str, tipo da unidade secionadora: 22 = chave fusível, 29 = distunjor, etc


        :return lista_UNSEMTs_sub: list[NamedTuple], lista dos dados das unidades seccionadoras
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT cod_id, pac_1, pac_2, fas_con, tip_unid, ctmt, uni_tr_s, p_n_ope, cap_elo, " +
                                "cor_nom, sit_ativ  FROM unsemt WHERE sub = '" + nomeSE_MT + "' AND tip_unid = '" +
                                tipoSEC + "' ORDER BY tip_unid")
                case "2021":
                    sqlQuery = ("SELECT cod_id, pac_1, pac_2, fas_con, tip_unid, ctmt, uni_tr_at, p_n_ope, cap_elo, " +
                                "cor_nom, sit_ativ  FROM unsemt WHERE sub = '" + nomeSE_MT + "' AND tip_unid = '" +
                                tipoSEC + "' ORDER BY tip_unid")

            UNSEMTs_sub = self.DataBaseConn.getSQLDB("UNSEMT", sqlQuery)

            lista_UNSEMTs_sub = []
            for UNSEMT_sub in UNSEMTs_sub.fetchall():
                tmp_dados = data.dadosSECMT(
                    UNSEMT_sub[0],  # cod_id
                    UNSEMT_sub[1],  # pac_1
                    UNSEMT_sub[2],  # pac_2
                    UNSEMT_sub[3],  # fas_con
                    UNSEMT_sub[4],  # tip_unid
                    UNSEMT_sub[5],  # ctmt
                    UNSEMT_sub[6],  # uni_tr_s
                    UNSEMT_sub[7],  # p_n_ope
                    UNSEMT_sub[8],  # cap_elo
                    UNSEMT_sub[9],  # cor_nom
                    UNSEMT_sub[10],  # sit_ativ
                )
                lista_UNSEMTs_sub.append(tmp_dados)

            return lista_UNSEMTs_sub

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para as Seccionadoras MT: " +
                                              str(tipoSEC))

    def getData_SegLinhasMT(self, nomeSE_MT):
        """
        Seleciona colunas do banco de SSDMT de uma determinada sub, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subestação


        :return lista_SSDMTs_sub: list[NamedTuple], lista dos dados das SSDMTs da subestação
        """
        start_time = time.time()
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT DISTINCT cod_id, ctmt, pac_1, pac_2, fas_con, comp, tip_cnd, uni_tr_s " +
                                "FROM ssdmt WHERE sub = '" + nomeSE_MT + "' ORDER BY ctmt")
                case "2021":
                    sqlQuery = ("SELECT DISTINCT cod_id, ctmt, pac_1, pac_2, fas_con, comp, tip_cnd, uni_tr_at " +
                                "FROM ssdmt WHERE sub = '" + nomeSE_MT + "' ORDER BY ctmt")

            SSDMTs_sub = self.DataBaseConn.getSQLDB("SSDMT", sqlQuery)

            lista_SSDMTs_sub = []
            for SSDMT_sub in SSDMTs_sub.fetchall():
                tmp_dados = data.dadosSegLinhas(
                    SSDMT_sub[0],  # cod_id
                    SSDMT_sub[1],  # ctmt
                    SSDMT_sub[2],  # pac_1
                    SSDMT_sub[3],  # pac_2
                    SSDMT_sub[4],  # fas_con
                    SSDMT_sub[5],  # comp
                    SSDMT_sub[6],  # tip_cnd
                    SSDMT_sub[7],  # uni_tr_s
                )
                lista_SSDMTs_sub.append(tmp_dados)
            print("--- %s seconds --- SSDMT" % (time.time() - start_time))
            return lista_SSDMTs_sub

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para os Segmentos de Linha! ")

    def getData_ReguladorMT(self, nomeSE_MT):
        """
        Seleciona colunas do banco de UNREMT de uma determinada sub, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subestação


        :return lista_UNREMTs_sub: list[NamedTuple], lista dos dados das unidades reguladoras
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT cod_id, ctmt, pac_1, pac_2, fas_con, sit_ativ, descr FROM unremt " +
                                "WHERE sub = '" + nomeSE_MT + "'")
                case "2021":
                    sqlQuery = ("SELECT cod_id, ctmt, pac_1, pac_2, fas_con, sit_ativ, descr FROM unremt " +
                                "WHERE sub = '" + nomeSE_MT + "'")

            UNREMTs_sub = self.DataBaseConn.getSQLDB("UNREMT", sqlQuery)

            lista_UNREMTs_sub = []
            for UNREMT_sub in UNREMTs_sub.fetchall():
                tmp_dados = data.dadosUNREMT(
                    UNREMT_sub[0],  # cod_id
                    UNREMT_sub[1],  # ctmt
                    UNREMT_sub[2],  # pac_1
                    UNREMT_sub[3],  # pac_2
                    UNREMT_sub[4],  # fas_con
                    UNREMT_sub[5],  # sit_ativ
                    UNREMT_sub[6],  # descr
                )
                lista_UNREMTs_sub.append(tmp_dados)
            return lista_UNREMTs_sub

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para os Reguladores de MT! ")

    def getData_UniConsumidora(self, nomeSE_MT, tipoUniCons):
        """
        Seleciona colunas do banco de unidades consumidoras de uma determinada sub, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subestação
        :param tipoUniCons: str, tipo de ligação da unidade consumidora: BT = baixa tensão e MT = média tensão


        :return lista_UNCs_sub: list[NamedTuple], lista dos dados das unidades consumidoras
        """
        start_time = time.time()
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    if tipoUniCons == "MT":
                        dbase = "UCMT"
                        uni_tr = "uni_tr_s"
                    elif tipoUniCons == "BT":
                        dbase = "UCBT"
                        uni_tr = "uni_tr_d"
                    else:
                        raise class_exception.ExecOpenDSS("Erro ao carregar as informações das Unidades Consumidoras!" +
                                                          "\nTipo não foi especificado! \n" + tipoUniCons)
                    sqlQuery = ("SELECT objectid, pac, ctmt, fas_con, ten_forn, sit_ativ, tip_cc, car_inst, ene_01, " +
                                "ene_02, ene_03, ene_04, ene_05, ene_06, ene_07, ene_08, ene_09, ene_10, ene_11, " +
                                "ene_12, uni_tr_s, " + uni_tr + " FROM " + dbase + " WHERE sub = '" + nomeSE_MT +
                                "' ORDER BY " + uni_tr)

                case "2021":
                    if tipoUniCons == "MT":
                        dbase = "UCMT"
                        uni_tr = "uni_tr_at"
                    elif tipoUniCons == "BT":
                        dbase = "UCBT"
                        uni_tr = "uni_tr_mt"
                    else:
                        raise class_exception.ExecOpenDSS("Erro ao carregar as informações das Unidades Consumidoras!" +
                                                          "\nTipo não foi especificado! \n" + tipoUniCons)
                    sqlQuery = ("SELECT objectid, pac, ctmt, fas_con, ten_forn, sit_ativ, tip_cc, car_inst, ene_01, " +
                                "ene_02, ene_03, ene_04, ene_05, ene_06, ene_07, ene_08, ene_09, ene_10, ene_11, " +
                                "ene_12, uni_tr_at, " + uni_tr + " FROM " + dbase + " WHERE sub = '" + nomeSE_MT +
                                "' ORDER BY " + uni_tr)

            UNCs_sub = self.DataBaseConn.getSQLDB(dbase, sqlQuery)

            lista_UNCs_sub = []
            for UNC_sub in UNCs_sub.fetchall():
                tmp_dados = data.dadosUnidCons(
                    UNC_sub[0],  # objectid
                    UNC_sub[1],  # pac
                    UNC_sub[2],  # ctmt
                    UNC_sub[3],  # fas_con
                    UNC_sub[4],  # ten_forn
                    UNC_sub[5],  # sit_ativ
                    UNC_sub[6],  # tip_cc
                    UNC_sub[7],  # car_inst
                    UNC_sub[8],  # ene_01
                    UNC_sub[9],  # ene_02
                    UNC_sub[10],  # ene_03
                    UNC_sub[11],  # ene_04
                    UNC_sub[12],  # ene_05
                    UNC_sub[13],  # ene_06
                    UNC_sub[14],  # ene_07
                    UNC_sub[15],  # ene_08
                    UNC_sub[16],  # ene_09
                    UNC_sub[17],  # ene_10
                    UNC_sub[18],  # ene_01
                    UNC_sub[19],  # ene_01
                    UNC_sub[20],  # uni_tr_s
                    UNC_sub[21]  # uni_tr
                )
                lista_UNCs_sub.append(tmp_dados)
            print("--- %s seconds UNI_c---" % (time.time() - start_time))
            return lista_UNCs_sub

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para as Unidades Consumidoras! ")

    def getData_TrafoDIST(self, codCT_MT):  # OBS: Verificar a existência de erros na ordem das keys nas NamedTuples
        """
        Seleciona colunas dos bancos de transformadores MT de um determinado alimentador, e as armazena em uma lista


        :param codCT_MT: str, código do circuito alimentador de média tensão


        :return lista_TRMTs_ctmt: list[NamedTuple], lista dos dados dos transformadores do alimentador
        """
        print('COMEÇANDO TRAFO')
        start_time = time.time()

        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQueryUNTRMT = ("SELECT cod_id, pac_1, pac_2, pac_3, fas_con_p, fas_con_s, fas_con_t, sit_ativ," +
                                      " tip_unid, ten_lin_se, cap_elo, cap_cha, tap, pot_nom, per_fer, per_tot, ctmt," +
                                      " tip_trafo, uni_tr_s FROM  untrd WHERE pac_1 LIKE '%" + codCT_MT +
                                      " ORDER BY pac_1")
                    sqlQueryEQTRMT = ("SELECT cod_id, pac_1, pac_2, pac_3, fas_con, pot_nom, lig, ten_pri, ten_sec, " +
                                      "ten_ter, lig_fas_p, lig_fas_s, lig_fas_t, per_fer, per_tot, r, xhl " +
                                      "FROM  eqtrd WHERE pac_1 LIKE '%" + codCT_MT + " ORDER BY pac_1")
                    UNTRMTs_ctmt = self.DataBaseConn.getSQLDB("UNTRD", sqlQueryUNTRMT)
                    EQTRMTs_ctmt = self.DataBaseConn.getSQLDB("EQTRD", sqlQueryEQTRMT)

                    lista_TRMTs_ctmt = []
                    for EQTRMT_ctmt, UNTRMT_ctmt in zip(EQTRMTs_ctmt.fetchall(), UNTRMTs_ctmt.fetchall()):
                        TRMT_ctmt = data.dadosTrafoDist(
                            UNTRMT_ctmt[0],  # cod_id
                            UNTRMT_ctmt[1],  # pac_1
                            UNTRMT_ctmt[2],  # pac_2
                            UNTRMT_ctmt[3],  # pac_3
                            UNTRMT_ctmt[4],  # fas_con_s
                            UNTRMT_ctmt[5],  # sit_ativ
                            UNTRMT_ctmt[6],  # ten_lin_se
                            UNTRMT_ctmt[7],  # pot_nom
                            UNTRMT_ctmt[8],  # ctmt
                            UNTRMT_ctmt[9],  # uni_tr_s
                            UNTRMT_ctmt[10],  # uni_tr_s
                            UNTRMT_ctmt[11],  # uni_tr_s
                            UNTRMT_ctmt[12],  # uni_tr_s
                            UNTRMT_ctmt[13],  # uni_tr_s
                            UNTRMT_ctmt[14],  # uni_tr_s
                            UNTRMT_ctmt[15],  # uni_tr_s
                            UNTRMT_ctmt[16],  # uni_tr_s
                            UNTRMT_ctmt[17],  # uni_tr_s
                            UNTRMT_ctmt[18],  # uni_tr_s
                            EQTRMT_ctmt[0],  # pot_nom_eqtrd
                            EQTRMT_ctmt[1],  # lig
                            EQTRMT_ctmt[2],  # ten_pri
                            EQTRMT_ctmt[3],  # ten_pri
                            EQTRMT_ctmt[4],  # ten_pri
                            EQTRMT_ctmt[5],  # ten_pri
                            EQTRMT_ctmt[6],  # ten_pri
                            EQTRMT_ctmt[7],  # ten_pri
                            EQTRMT_ctmt[8],  # ten_pri
                            EQTRMT_ctmt[9],  # ten_pri
                            EQTRMT_ctmt[10],  # ten_pri
                            EQTRMT_ctmt[11],  # ten_pri
                            EQTRMT_ctmt[12],  # ten_pri
                            EQTRMT_ctmt[13],  # ten_pri
                            EQTRMT_ctmt[14],  # r
                            EQTRMT_ctmt[15],  # xhl
                            EQTRMT_ctmt[16]  # ten_pri
                        )
                        lista_TRMTs_ctmt.append(TRMT_ctmt)

                case "2021":
                    sqlQueryUNTRMT = ("SELECT cod_id, pac_1, pac_2, pac_3, fas_con_p, fas_con_s, fas_con_t, sit_ativ," +
                                      " tip_unid, ten_lin_se, cap_elo, cap_cha, tap, pot_nom, per_fer, per_tot, ctmt," +
                                      " tip_trafo, uni_tr_at FROM untrmt WHERE pac_1 LIKE '%" + codCT_MT +
                                      " ORDER BY cod_id")
                    UNTRMTs_ctmt = self.DataBaseConn.getSQLDB("UNTRMT", sqlQueryUNTRMT)
                    cod_id_untrmts = [untrmt[0] for untrmt in UNTRMTs_ctmt.fetchall()]
                    sqlQueryEQTRMT = ("SELECT cod_id, fas_con, pot_nom, lig, ten_pri, ten_sec, ten_ter, lig_fas_p, " +
                                      "lig_fas_s, lig_fas_t, per_fer, per_tot, r, xhl FROM eqtrmt WHERE uni_tr_mt " +
                                      "IN(" + str(cod_id_untrmts)[1:-1] + ") ORDER BY uni_tr_mt")
                    EQTRMTs_ctmt = self.DataBaseConn.getSQLDB("EQTRMT", sqlQueryEQTRMT)

                    lista_TRMTs_ctmt = []
                    for EQTRMT_ctmt, UNTRMT_ctmt in zip(EQTRMTs_ctmt.fetchall(), UNTRMTs_ctmt.fetchall()):
                        TRMT_ctmt = data.dadosTrafoDist(
                            UNTRMT_ctmt[0],  # cod_id
                            UNTRMT_ctmt[1],  # pac_1
                            UNTRMT_ctmt[2],  # pac_2
                            UNTRMT_ctmt[3],  # pac_3
                            UNTRMT_ctmt[4],  # fas_con_s
                            UNTRMT_ctmt[5],  # sit_ativ
                            UNTRMT_ctmt[6],  # ten_lin_se
                            UNTRMT_ctmt[7],  # pot_nom
                            UNTRMT_ctmt[8],  # ctmt
                            UNTRMT_ctmt[9],  # uni_tr_s
                            UNTRMT_ctmt[10],  # uni_tr_s
                            UNTRMT_ctmt[11],  # uni_tr_s
                            UNTRMT_ctmt[12],  # uni_tr_s
                            UNTRMT_ctmt[13],  # uni_tr_s
                            UNTRMT_ctmt[14],  # uni_tr_s
                            UNTRMT_ctmt[15],  # uni_tr_s
                            UNTRMT_ctmt[16],  # uni_tr_s
                            UNTRMT_ctmt[17],  # uni_tr_s
                            UNTRMT_ctmt[18],  # uni_tr_s
                            EQTRMT_ctmt[0],  # pot_nom_eqtrd
                            UNTRMT_ctmt[1],  # lig
                            UNTRMT_ctmt[2],  # ten_pri
                            UNTRMT_ctmt[3],  # ten_pri
                            EQTRMT_ctmt[1],  # ten_pri
                            EQTRMT_ctmt[2],  # ten_pri
                            EQTRMT_ctmt[3],  # ten_pri
                            EQTRMT_ctmt[4],  # ten_pri
                            EQTRMT_ctmt[5],  # ten_pri
                            EQTRMT_ctmt[6],  # ten_pri
                            EQTRMT_ctmt[7],  # ten_pri
                            EQTRMT_ctmt[8],  # ten_pri
                            EQTRMT_ctmt[9],  # ten_pri
                            EQTRMT_ctmt[10],  # ten_pri
                            EQTRMT_ctmt[11],  # r
                            EQTRMT_ctmt[12],  # xhl
                            EQTRMT_ctmt[13]  # ten_pri
                        )
                        lista_TRMTs_ctmt.append(TRMT_ctmt)

            print("--- %s seconds --- TRAFO_DIST" % (time.time() - start_time))
            return lista_TRMTs_ctmt

        except:
            raise class_exception.ExecOpenDSS("Erro no processamento do Banco de Dados para os Transformadores " +
                                              "de Distribuição! ")

    def getData_SegLinhasRamLigBT(self, nomeSE_MT, tipoLinha):
        """
        Seleciona colunas dos bancos de transformadores MT de um determinado alimentador, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subestação
        :param tipoLinha: str, tipo de linha: SEGBT = segmento de baixa tensão e RLIG = ramal de ligação


        :return lista_SEGBTs_sub: list[NamedTuple], lista dos dados dos segmentos de linha
        """
        start_time = time.time()
        try:
            if tipoLinha == "SEGBT":  # Segmentos de Linhas de Baixa Tensão
                dbase = "SSDBT"
            elif tipoLinha == "RLIG":  # Ramal de Ligação
                dbase = "RAMLIG"
            else:
                raise class_exception.ExecOpenDSS("Erro ao carregar as informações das linhas de BT!\nTipo não foi" +
                                                  "especificado! \n" + tipoLinha)

            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT DISTINCT cod_id, ctmt, pac_1, pac_2, fas_con, comp, tip_cnd, uni_tr_d FROM " +
                                dbase + " WHERE sub = '" + nomeSE_MT + "'")

                case "2021":
                    sqlQuery = ("SELECT DISTINCT cod_id, ctmt, pac_1, pac_2, fas_con, comp, tip_cnd, uni_tr_mt FROM " +
                                dbase + " WHERE sub = '" + nomeSE_MT + "'")

            SEGBTs_sub = self.DataBaseConn.getSQLDB(dbase, sqlQuery)

            lista_SEGBTs_sub = []
            for SEGBT_sub in SEGBTs_sub.fetchall():
                tmp_dados = data.dadosSegLinhas(
                    SEGBT_sub[0],  # cod_id
                    SEGBT_sub[1],  # ctmt
                    SEGBT_sub[2],  # pac_1
                    SEGBT_sub[3],  # pac_2
                    SEGBT_sub[4],  # fas_con
                    SEGBT_sub[5],  # comp
                    SEGBT_sub[6],  # tip_cnd
                    SEGBT_sub[7],  # uni_tr_d
                )

                lista_SEGBTs_sub.append(tmp_dados)
            print("--- %s seconds --- SSDBT" % (time.time() - start_time))
            return lista_SEGBTs_sub

        except:
            raise class_exception.ExecOpenFDSS("Erro no processamento do Banco de Dados das Linhas de BT e" +
                                               "Ramais de Ligação!\n" + tipoLinha)

    def getData_UniCompReativo(self, nomeSE_MT, tipoCAP):  # Pega os reguladores
        """
        Seleciona colunas do banco de unidades compensadoras de uma determinada sub, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subestação
        :param tipoCAP: str, tipo de unidade compensadora: BT = baixa tensão e MT = média tensão


        :return lista_UNCRs_sub: list[NamedTuple], lista dos dados das unidades compensadoras da subestação
        """
        try:
            if tipoCAP == "MT":  # Média Tensão
                dbase = "UNCRMT"
            elif tipoCAP == "BT":  # Ramal de Ligação
                dbase = "UNCRBT"
            else:
                raise class_exception.ExecOpenDSS("Erro ao carregar as informações dos compensadores de reativo!" +
                                                  "\nTipo não foi especificado! \n" + tipoCAP)

            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT cod_id, fas_con, pot_nom, pac_1, ctmt FROM " + dbase + " WHERE sub = '" +
                                nomeSE_MT + "'")
                case "2021":
                    sqlQuery = ("SELECT cod_id, fas_con, pot_nom, pac_1, ctmt FROM " + dbase + " WHERE sub = '" +
                                nomeSE_MT + "'")
            UNCRs_sub = self.DataBaseConn.getSQLDB(dbase, sqlQuery)

            lista_UNCRs_sub = []
            for UNCR_sub in UNCRs_sub.fetchall():
                tmp_dados = data.dadosUnidCompReat(
                    UNCR_sub[0],  # cod_id
                    UNCR_sub[1],  # fas_con
                    UNCR_sub[2],  # pot_nom
                    UNCR_sub[3],  # pac_1
                    UNCR_sub[4],  # ctmt
                )

                lista_UNCRs_sub.append(tmp_dados)

            return lista_UNCRs_sub

        except:
            raise class_exception.ExecOpenDSS(
                "Erro no processamento do Banco de Dados dos Compensadores de Reativo!\n" +
                tipoCAP)
