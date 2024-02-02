from typing import List, Any

import database.class_conn
import class_exception
import prodist.tpos
import prodist.ttranf
import prodist.tpostotran

class C_DBaseCoord():
    def __init__(self):
        self._DataBaseConn = database.class_conn.C_DBaseConn()

    @property
    def DataBaseConn(self):
        return self._DataBaseConn

    @DataBaseConn.setter
    def DataBaseConn(self, value):
        self._DataBaseConn = value


    def getCods_AL_SE_MT_DB(self, listaNomesAL_MT):
        """
        Seleciona o cod_id do banco de CTMT com base no nome do CTMT, e as armazena em uma lista


        :param listaNomesAL_MT: str, lista de nomes dos alimentadores de média tensão (ctmts)


        :return lista_de_identificadores_dos_alimentadores: list, lista dos códigos_id dos alimentadores
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT DISTINCT cod_id FROM ctmt WHERE nom IN(" + str(listaNomesAL_MT)[1:-1] +
                                ") ORDER BY nom")
                case "2021":
                    sqlQuery = ("SELECT DISTINCT cod_id FROM ctmt WHERE nome IN(" + str(listaNomesAL_MT)[1:-1] +
                                ") ORDER BY nome")

            ctmts = self.DataBaseConn.getSQLDB("CTMT", sqlQuery)

            lista_de_identificadores_dos_alimentadores = [ctmt[0] for ctmt in ctmts.fetchall()]

            return lista_de_identificadores_dos_alimentadores
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Códigos dos Alimentadores de Média Tensão!")

    def getCoord_AL_SE_MT_DB(self, nomeAL_MT):
        """
        Seleciona colunas dos bancos de SSDMT de um determinado ctmt, e as armazena em uma lista


        :param nomeAL_MT: str, nome do alimentador de média tensão (ctmts)


        :return lista_de_coordenadas_do_alimentador: list[list[list[int]]], lista das coordenadas dos alimentadores
        :return lista_de_dados_linha: list[str], lista dos pac's dos alimentadores
        """
        try:
            nomeAL_MTS = []
            nomeAL_MTS.append(str(nomeAL_MT))
            codAlimentador = self.getCods_AL_SE_MT_DB(nomeAL_MTS)

            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT DISTINCT ctmt, x, y, vertex_index, pac_1, objectid FROM ssdmt WHERE ctmt ='" +
                                str(codAlimentador[0]) + "' ORDER BY objectid")
                case "2021":
                    sqlQuery = ("SELECT DISTINCT ctmt, x, y, vertex_index, pac_1, objectid FROM ssdmt WHERE ctmt ='" +
                                str(codAlimentador[0]) + "' ORDER BY objectid")

            cod_al = self.DataBaseConn.getSQLDB("SSDMT", sqlQuery)

            lista_de_coordenadas_do_alimentador: list[list[list[int]]] = []
            lista_de_dados_linha: list[str] = []
            for linha in cod_al.fetchall():
                    if linha[3] == 0:
                        dadosCoordInicio = [linha[2], linha[1]]
                        continue

                    dadosCoordFim = [linha[2], linha[1]]

                    dadosCoord: list[list[int]] = [dadosCoordInicio, dadosCoordFim]
                    lista_de_coordenadas_do_alimentador.append(dadosCoord)

                    dadoslinha = str(linha[4])
                    lista_de_dados_linha.append(dadoslinha)

            return lista_de_coordenadas_do_alimentador, lista_de_dados_linha

        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar as Coordenadas dos Alimentadores de Média Tensão!")

    def getCoord_AL_SE_MT_BT_DB(self, codTD): #Pega as coordenadas dos circuitos de baixa de um alimentador
        """
        Seleciona colunas dos bancos de SSDBT de uma determinada untrmt, e as armazena em uma lista


        :param codTD: str, código do transformador alimentador de média tensão


        :return lista_de_coordenadas_BT: list[list[list[int]]], lista das coordenadas dos segmentos de baixa tensão
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT DISTINCT ctmt, x, y, vertex_index, objectid FROM ssdbt WHERE uni_tr_d ='" +
                                codTD + "' ORDER BY objectid")
                case "2021":
                    sqlQuery = ("SELECT DISTINCT ctmt, x, y, vertex_index, objectid FROM ssdbt WHERE uni_tr_mt ='" +
                                codTD + "' ORDER BY objectid")

            cod_al = self.DataBaseConn.getSQLDB("SSDBT", sqlQuery)

            lista_de_coordenadas_BT: list[list[list[int]]] = []
            for linha in cod_al.fetchall():
                    if linha[3] == 0:
                        dadosCoordInicio = [linha[2], linha[1]]
                        continue

                    dadosCoordFim = [linha[2], linha[1]]

                    dadosCoord: list[list[Any]] = [dadosCoordInicio, dadosCoordFim]
                    lista_de_coordenadas_BT.append(dadosCoord)

            return lista_de_coordenadas_BT

        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar as Coordenadas dos Alimentadores de Baixa Tensão!")


    def getData_TrafoDIST(self, nomeSE_MT, codCT_MT):
        """
        Seleciona colunas dos bancos de TRs_MT de uma determinada sub e ctmt, e as armazena em uma lista


        :param nomeSE_MT: str, nome da subesteação escolhida
        :param codCT_MT: str, código do circuito alimentador de média tensão


        :return lista_UNTRMTs: list[list[Any]], lista dos dados dos transformadores de média tensão
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery = ("SELECT cod_id, pot_nom, ctmt, x, y, tip_trafo, pos, posto, pac_1, pac_2 FROM untrd " +
                                "WHERE sub = '" + nomeSE_MT[0] + "' AND ctmt = '" + codCT_MT + "'")
                    UNTRMTs = self.DataBaseConn.getSQLDB("UNTRD", sqlQuery)

                case "2021":
                    sqlQuery = ("SELECT cod_id, pot_nom, ctmt, x, y, tip_trafo, pos, posto, pac_1, pac_2 FROM untrmt " +
                                "WHERE sub = '" + nomeSE_MT[0] + "' AND ctmt = '" + codCT_MT + "'")
                    UNTRMTs = self.DataBaseConn.getSQLDB("UNTRMT", sqlQuery)

            lista_UNTRMTs = [[UNTRMT[4],                                            # y
                              UNTRMT[3],                                            # x
                              UNTRMT[0],                                            # cod_id
                              UNTRMT[1],                                            # pot_nom
                              prodist.ttranf.TTRANF[UNTRMT[5]],                     # tip_trafo
                              prodist.tpos.TPOS[UNTRMT[6]],                         # pos
                              prodist.tpostotran.TPOSTOTRAN[UNTRMT[7]],             # posto
                              UNTRMT[8],                                            # pac_1
                              UNTRMT[9]] for UNTRMT in UNTRMTs.fetchall()]          # pac_2

            return lista_UNTRMTs

        except:
            raise class_exception.ExecData("Erro no processamento do Banco de Dados para os Transformadores " +
                                           "de Distribuição! ")


    def getData_UniConsumidoraMT(self, nomeSE_MT, codCT_MT):
        """
        Seleciona colunas dos bancos de UCMT de uma determinada sub e ctmt, também puxa as coordenadas aproximadas
        da UCMT através de uma comparação com o banco SSDMT, e as armazena em uma lista

        :param nomeSE_MT: str, nome da subesteação escolhida
        :param codCT_MT: str, código do circuito alimentador de média tensão


        :return lista_UCMTs: list[list[Any]], lista dos dados das unidades consumidoras de média tensão
        """
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    sqlQuery_ssdmt = ("SELECT pn_con_1, pn_con_2, x, y FROM  ssdmt WHERE sub = '" + nomeSE_MT[0] +
                                      "' AND ctmt = '" + codCT_MT + "'")
                    sqlQuery_ucmt = ("SELECT pn_con, brr, sit_ativ, car_inst, dat_con, ctmt FROM ucmt WHERE sub = '" +
                                     nomeSE_MT[0] + "' AND ctmt = '" + codCT_MT + "'")
                case "2021":
                    sqlQuery_ssdmt = ("SELECT pn_con_1, pn_con_2, x, y FROM  ssdmt WHERE sub = '" + nomeSE_MT[0] +
                                      "' AND ctmt = '" + codCT_MT + "'")
                    sqlQuery_ucmt = ("SELECT pn_con, brr, sit_ativ, car_inst, dat_con, ctmt FROM ucmt WHERE sub = '" +
                                     nomeSE_MT[0] + "' AND ctmt = '" + codCT_MT + "'")

            SSDMTs = self.DataBaseConn.getSQLDB("SSDMT", sqlQuery_ssdmt)
            UCMTs = self.DataBaseConn.getSQLDB("UCMT", sqlQuery_ucmt)

            lista_ssdmts: list[list[Any]] = [[SSDMT[0],                                     # pn_con_1
                                              SSDMT[1],                                     # pn_con_2
                                              SSDMT[2],                                     # x
                                              SSDMT[3]] for SSDMT in SSDMTs.fetchall()]     # y

            lista_UCMTs = []
            for UCMT in UCMTs.fetchall():  # Pegando o Transformador

                tmp_dados = []
                for SSDMT in lista_ssdmts:
                    if (SSDMT[0] == UCMT[0]) or (SSDMT[1] == UCMT[0]):

                        tmp_dados = [SSDMT[3],  # y
                                     SSDMT[2],  # x
                                     UCMT[1],   # brr
                                     UCMT[2],   # sit_ativ
                                     UCMT[3],   # car_inst
                                     UCMT[4]]   # dat_con

                        lista_UCMTs.append(tmp_dados)
                        break

            return lista_UCMTs
        except:
            raise class_exception.ExecData("Erro no processamento do Banco de Dados para as Unidades Consumidoras " +
                                           "de Média Tensão! ")
