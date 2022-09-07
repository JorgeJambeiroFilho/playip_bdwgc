import pyodbc
from dynaconf import settings

server = settings.SERVER
database = settings.DATABASE
username = settings.USERNAME
password = settings.PASSWORD

cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
cursor.execute("""
            SELECT  
                    Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
                    Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO as bairro, Cidade.ID_LOCALIDADE as id_cidade, 
                    Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf, tmeio.TX_DESCRICAO_TIPO        
            FROM             
                    Contrato_PacoteServico_Servico as cps 
                    INNER JOIN Contrato as Contrato on (cps.ID_CONTRATO = Contrato.ID_CONTRATO)
                    INNER JOIN PacoteServico as ps on (cps.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO) 
                    INNER JOIN PacoteServico_Servico as s on (ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO) 
                    
                    INNER JOIN Servico_DICI as dici on (dici.ID_SERVICO=cps.ID_SERVICO)
                    INNER JOIN TiposDiversos as tmeio on (tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO) 
                            
                    INNER JOIN Endereco as Endereco on (Endereco.ID_ENDERECO=Contrato.ID_ENDERECO_INSTALACAO)
                    LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                    LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                    LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)
            ORDER BY 
                    UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.TX_ENDERECO, Endereco.NR_NUMERO, Endereco.TX_COMPLEMENTO
        """)
columns = [column[0] for column in cursor.description]
print(columns)
row = cursor.fetchone()
while row:
    r = [str(x).replace(","," ") if x is not None else "None" for x in row]
    print(r)
    row = cursor.fetchone()


