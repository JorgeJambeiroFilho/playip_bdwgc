import pyodbc
import datetime

server = '170.238.84.12'
database = 'WGC'
username = 'wgcplayip'
password = 'teste01*'
cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
cursor.execute("""
            SELECT  
                ci.ID_CONTRATO_ITEM as ID_CONTRATO_ITEM,
                ci.ID_PACOTE_SERVICO as ID_PACOTE_SERVICO,
                contrato.ID_CONTRATO as ID_CONTRATO,
                contrato.DT_INICIO as CONTRATO_DT_INICIO,
                contrato.DT_FIM as CONTRATO_DT_FIM,
                contrato.DT_ATIVACAO as CONTRATO_DT_ATIVACAO,
                contrato.DT_CANCELAMENTO as CONTRATO_DT_CANCELAMENTO,

                cps.DT_ATIVACAO as SERVICO_DT_ATIVACAO, 
                cps.DT_DESATIVACAO as SERVICO_DT_DESATIVACAO, 
                cps.DT_DESISTENCIA as SERVICO_DT_DESISTENCIA, 
                cps.TX_MOTIVO_CANCELAMENTO as SERVICO_TX_MOTIVO_CANCELAMENTO, 
                cps.VL_SERVICO as VL_PACOTE_SERVICO,
                cps.ID_CONTRATO_PACOTESERVICO_SERVICO as ID_CONTRATO_PACOTESERVICO_SERVICO,

                s.VL_DOWNLOAD as VL_DOWNLOAD, 
                s.VL_UPLOAD as VL_UPLOAD, 
                s.VL_SERVICO as VL_SERVICO,
                ser.NM_SERVICO as NM_SERVICO,
                ser.VL_REFERENCIA as VL_REFERENCIA_SERVICO,

                dici.ID_TIPO_MEIO_ACESSO as ID_TIPO_MEIO_ACESSO, 
                dici.ID_TIPO_TECNOLOGIA as ID_TIPO_TECNOLOGIA, 
                dici.ID_TIPO_PRODUTO as ID_TIPO_PRODUTO, 
                tmeio.TX_DESCRICAO_TIPO as NM_MEIO, 
                ttec.TX_DESCRICAO_TIPO as NM_TEC, 
                tprod.TX_DESCRICAO_TIPO as NM_PROD, 

                ps.NM_PACOTE_SERVICO as NM_PACOTE_SERVICO, 
                ps.VL_PACOTE as VL_PACOTE,

                Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
                Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO as bairro, Cidade.ID_LOCALIDADE as id_cidade, 
                Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf


            FROM 
                ContratoItem as ci 
                INNER JOIN Contrato as contrato on (ci.ID_CONTRATO=contrato.ID_CONTRATO)
                INNER JOIN PacoteServico as ps on (ci.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO) 
                INNER JOIN PacoteServico_Servico as s on (ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO)
                INNER JOIN Servico as ser on (ser.ID_SERVICO=s.ID_SERVICO)  
                INNER JOIN Contrato_PacoteServico_Servico as cps on (cps.ID_SERVICO=s.ID_SERVICO and cps.ID_CONTRATO=ci.ID_CONTRATO)
                INNER JOIN Servico_DICI as dici on (dici.ID_SERVICO=cps.ID_SERVICO)
                INNER JOIN TiposDiversos as tmeio on (tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO) 
                INNER JOIN TiposDiversos as ttec on (ttec.ID_TIPO_DIVERSOS=dici.ID_TIPO_TECNOLOGIA) 
                INNER JOIN TiposDiversos as tprod on (tprod.ID_TIPO_DIVERSOS=dici.ID_TIPO_PRODUTO) 

                INNER JOIN Endereco as Endereco on (Endereco.ID_ENDERECO=Contrato.ID_ENDERECO_INSTALACAO)
                LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)

            WHERE
                tprod.TX_DESCRICAO_TIPO = 'internet'  
            ORDER BY 
                UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.NR_NUMERO, Endereco.TX_COMPLEMENTO,
                SERVICO_DT_ATIVACAO, ID_CONTRATO_PACOTESERVICO_SERVICO
            OFFSET 0 ROWS 
            FETCH FIRST 10 ROWS ONLY;
        """)
columns = [column[0] for column in cursor.description]
print(columns)
p = 0
row = cursor.fetchone()
while row:
    prow = []
    for v in row:
        if isinstance(v, datetime.datetime) or isinstance(v, datetime.date) :
            v = str(v.year) + "-" + str(v.month) + "-" + str(v.day)
        elif isinstance(v, str):
            v = v.replace(",", " ")
        prow.append(v)
    print(prow)
    row = cursor.fetchone()
    p += 1

#print("count ", p)