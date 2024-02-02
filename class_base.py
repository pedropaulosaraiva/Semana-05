import database.class_conn
import class_exception


class C_DBase:
    def __init__(self):
        self._DataBaseConn = database.class_conn.C_DBaseConn()  # Criando a instância do Banco de Dados

    @property
    def DataBaseConn(self):
        return self._DataBaseConn

    @DataBaseConn.setter
    def DataBaseConn(self, value):
        self._DataBaseConn = value

    # Métodos Novos
    def getSE_AT_DB(self):
        # Seleciona e salva as subestações presentes nos CTMT numa lista em ordem alfabética
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    print("Certo 1")
                    sqlQuery = "SELECT DISTINCT sub FROM ctmt ORDER BY sub"
                case "2021":
                    sqlQuery = "SELECT DISTINCT sub FROM ctmt ORDER BY sub"

            subs = self.DataBaseConn.getSQLDB("CTMT", sqlQuery)

            lista_de_subestacoes_de_alta_tensao = [sub[0] for sub in subs.fetchall()]

            return lista_de_subestacoes_de_alta_tensao
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Circuitos de Alta Tensão!")

    def getCirAT_MT_DB(self, nomeSE_AT):
        # Seleciona e salva os CTAT de uma determinada SUB numa lista em ordem alfabética e sem sufixos numéricos
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    print("Certo 2")
                    sqlQuery = ("SELECT DISTINCT nom FROM ctat WHERE nom LIKE '" + nomeSE_AT +
                                "%' AND NOT (nom LIKE '%2' OR nom LIKE '%3' OR nom LIKE'%4') ORDER BY nom")
                case "2021":
                    sqlQuery = ("SELECT DISTINCT nome FROM ctat WHERE nome LIKE '" + nomeSE_AT +
                                "%' AND NOT (nome LIKE '%2' OR nome LIKE '%3' OR nome LIKE'%4') ORDER BY nome")

            ctats = self.DataBaseConn.getSQLDB("CTAT", sqlQuery)

            lista_de_circuitos_de_alta_para_media = [ctat[0] for ctat in ctats.fetchall()]

            return lista_de_circuitos_de_alta_para_media
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Circuitos de Média Tensão!")

    def getSE_MT_AL_DB(self, nomeSE_MT):
        # Seleciona e salva os CTMT de uma determinada SUB numa lista em ordem alfabética
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    print("Certo 3")
                    sqlQuery = ("SELECT DISTINCT nom, cod_id FROM ctmt WHERE sub = '" + nomeSE_MT[0] + "'ORDER BY nom")
                case "2021":
                    sqlQuery = ("SELECT DISTINCT nome, cod_id FROM ctmt WHERE sub = '" + nomeSE_MT[
                        0] + "'ORDER BY nome")

            ctmts = self.DataBaseConn.getSQLDB("CTMT", sqlQuery)  # OBS: nomeSE_MT é uma lista

            lista_de_alimentadores_de_media_tensao = [ctmt for ctmt in ctmts.fetchall()]

            return lista_de_alimentadores_de_media_tensao
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Alimentadores de Média Tensão!")

    def getSE_MT_AL_TrafoDIST(self, codField):
        # Seleciona e salva os tranformadores de distribuição presentes num CTMT numa lista
        try:
            match self.DataBaseConn.DataBaseInfo["versao"]:
                case "2017":
                    print("Certo 4")
                    sqlQuery = ("SELECT DISTINCT cod_id FROM untrd WHERE ctmt = '" + codField +
                                "' AND pos = 'PD' ORDER BY cod_id")
                    untrds = self.DataBaseConn.getSQLDB("UNTRD", sqlQuery)
                case "2021":
                    sqlQuery = ("SELECT DISTINCT cod_id FROM untrmt WHERE ctmt = '" + codField +
                                "' AND pos = 'PD' ORDER BY cod_id")
                    untrds = self.DataBaseConn.getSQLDB("UNTRMT", sqlQuery)

            lista_transformadores_de_distribuicao = [untrd[0] for untrd in untrds]

            return lista_transformadores_de_distribuicao
        except:
            raise class_exception.ExecData("Erro no processamento do Banco de Dados para os Transformadores de " +
                                           "Distribuição!")
