import datetime
from asyncio import Condition
from typing import Optional
from typing import List
from dynaconf import settings
from fastapi import APIRouter, Depends
import pydantic
import fastapi
import pyodbc

from playipappcommons.auth.oauth2FastAPI import defaultpermissiondep
from playipappcommons.infra.endereco import Endereco
from playipappcommons.ispbd.ispbddata import Client, ContractData

print("FASTAPI version ",fastapi.__version__)

wgcrouter = APIRouter(prefix="/playipispbd/basic")

gwbd = None
condBD = Condition()
bdConnectionNumber = 0
timeConnect = 0

async def getWDB():
    global gwbd
    global timeConnect
    async with condBD:
        if not gwbd:
            gwbd = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+settings.SERVER+';DATABASE='+settings.DATABASE+';UID='+settings.USERNAME+';PWD='+ settings.PASSWORD)
            timeConnect = datetime.datetime.now().timestamp()
        return gwbd

async def renewWDB(num):
    global gwbd
    global bdConnectionNumber
    global timeConnect
    time = datetime.datetime.now().timestamp()
    async with condBD:
        # Se a conexão atual falhou, a invalida para forçar reconexão
        # Para evitar que falhas quase simultâneas resultem em várias conexoes, verifica se a conexao usada foi memso a última
        # Evita tentar de reconectar vária vezes por segundo. Em lugar disso deixa alguns casos falharem.
        if num >= bdConnectionNumber and time - timeConnect > 1000:
            try:
                gwbd.close()
            except:
                pass
            gwbd = None
            bdConnectionNumber += 1




@wgcrouter.get("/getclientfromcpfcnpj/{cpfcnpj}", response_model=Client)
async def getClientFromCPFCNPJ(cpfcnpj:str, auth=Depends(defaultpermissiondep)) -> Client:
    global gwbd
    global bdConnectionNumber
    num = bdConnectionNumber
    try:
        return await getClientFromCPFCNPJInternal(cpfcnpj, auth)
    except pyodbc.Error as pe:
        print("Error:", pe)
        if pe.args[0] == "08S01":  # Communication error.
            await renewWDB(num)
            return await getClientFromCPFCNPJInternal(cpfcnpj, auth)
        else:
            raise  # Re-raise any other exception



async def getClientFromCPFCNPJInternal(cpfcnpj:str, auth=Depends(defaultpermissiondep)) -> Client:
    wdb = await getWDB()

    if len(cpfcnpj) == 11:
        with wdb.cursor() as cursor:
            cursor.execute("""
                SELECT  
                        Cliente.ID_CLIENTE, Cliente.ID_PESSOA, Pessoa.NM_PESSOA, PessoaFisica.TX_CPF
                FROM 
                        Cliente 
                        INNER JOIN Pessoa on (Cliente.ID_PESSOA=Pessoa.ID_PESSOA)
                        INNER JOIN PessoaFisica on (Cliente.ID_PESSOA=PessoaFisica.ID_PESSOA)
                        
                WHERE 
                        PessoaFisica.TX_CPF = '{param_cpf}'                  
            """.format(param_cpf=cpfcnpj))
            row = cursor.fetchone()
            if row:
                id_client: str = row[0]
                name: str = row[2]
                cpfcnpj: str = cpfcnpj
                client: Client = Client(id_client=id_client, name=name, cpfcnpj=cpfcnpj, found=True)
                return client
            else:
                client: Client = Client()
                return client
    else:
        with wdb.cursor() as cursor:
            cursor.execute("""
                SELECT  
                        Cliente.ID_CLIENTE, Cliente.ID_PESSOA, Pessoa.NM_PESSOA, PessoaJuridica.NM_FANTASIA, PessoaJuridica.TX_CNPJ
                FROM 
                        Cliente 
                        INNER JOIN Pessoa on (Cliente.ID_PESSOA=Pessoa.ID_PESSOA)
                        INNER JOIN PessoaJuridica on (Cliente.ID_PESSOA=PessoaJuridica.ID_PESSOA)
                        
                WHERE 
                        PessoaJuridica.TX_CNPJ = '{param_cnpj}'                  
            """.format(param_cnpj=cpfcnpj))
            row = cursor.fetchone()
            if row:
                id_client: str = row[0]
                name: str = row[2]
                alt_name: str = row[3]
                cpfcnpj: str = cpfcnpj
                client: Client = Client(id_client=id_client, name=name, alt_name=alt_name, cpfcnpj=cpfcnpj, found=True)
                return client
            else:
                client: Client = Client()
                return client

@wgcrouter.get("/getcontracts/{id_client}", response_model=List[ContractData])
async def getContracts(id_client: str, auth=Depends(defaultpermissiondep)) -> List[ContractData]:
    wdb = await getWDB()
    contract_list: List[str] = []

    with wdb.cursor() as cursor:
        cursor.execute("""
            SELECT  
                    Cliente.ID_CLIENTE, Contrato.ID_CONTRATO
            FROM 
                    Cliente 
                    INNER JOIN Contrato on (Contrato.ID_CLIENTE=Cliente.ID_CLIENTE)
                    
            WHERE 
                    Cliente.ID_CLIENTE = {param_id_cliente}
                         """.format(param_id_cliente=id_client))
        row = cursor.fetchone()
        while row:
            contract_list.append(row[1])
            row = cursor.fetchone()

    contracts: List[ContractData] = []
    for c in contract_list:
        contract: ContractData = await getContract(c)
        contracts.append(contract)
    return contracts

@wgcrouter.get("/getcontractsfromcpfcnpj/{cpfcnpj}", response_model=List[ContractData])
async def getContractsFromCPFCNPJ(cpfcnpj: str, auth=Depends(defaultpermissiondep)) -> List[ContractData]:
    client: Client = await getClientFromCPFCNPJ(cpfcnpj)
    if client:
        contracts: List[ContractData] = await getContracts(client.id_client)
        return contracts
    else:
        return []

oldQuery = \
"""

          SELECT  NM_PACOTE_SERVICO, VL_DOWNLOAD, VL_UPLOAD, s.VL_SERVICO, cps.DT_ATIVACAO, cps.DT_DESATIVACAO, 
                  dici.ID_TIPO_MEIO_ACESSO, dici.ID_TIPO_TECNOLOGIA, dici.ID_TIPO_PRODUTO, 
                  tmeio.TX_DESCRICAO_TIPO, ttec.TX_DESCRICAO_TIPO, tprod.TX_DESCRICAO_TIPO, uname.TX_USERNAME,
                  bloq.ID_CONTRATO as bloq_id
          FROM 
                  ContratoItem as ci 
                  INNER JOIN PacoteServico as ps on (ci.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO) 
                  INNER JOIN PacoteServico_Servico as s on (ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO) 
                  INNER JOIN Contrato_PacoteServico_Servico as cps on (cps.ID_SERVICO=s.ID_SERVICO and cps.ID_CONTRATO=ci.ID_CONTRATO)
                  INNER JOIN Servico_DICI as dici on (dici.ID_SERVICO=cps.ID_SERVICO)
                  INNER JOIN TiposDiversos as tmeio on (tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO) 
                  INNER JOIN TiposDiversos as ttec on (ttec.ID_TIPO_DIVERSOS=dici.ID_TIPO_TECNOLOGIA) 
                  INNER JOIN TiposDiversos as tprod on (tprod.ID_TIPO_DIVERSOS=dici.ID_TIPO_PRODUTO) 
                  INNER JOIN UserName as uname on (ci.ID_CONTRATO=uname.ID_CONTRATO)
                  LEFT JOIN CONTRATOS_BLOQUEADOS as bloq on (bloq.ID_CONTRATO=ci.ID_CONTRATO)
          WHERE 
                ci.ID_CONTRATO={param_id_contrato} and 
                uname.dt_desativacao is null and
                s.VL_DOWNLOAD > 0
          ORDER BY
                    DT_ATIVACAO DESC  

"""


@wgcrouter.get("/getcontract/{id_contract}", response_model=ContractData)
async def getContract(id_contract:str, auth=Depends(defaultpermissiondep)) -> ContractData:
    wdb = await getWDB()

    #existe uma tabela chamada Servico que tinha tudo para ser relevante aqui, mas não foi
    with wdb.cursor() as cursor:
        cursor.execute("""
          SELECT  
                  NM_PACOTE_SERVICO, s.VL_DOWNLOAD, s.VL_UPLOAD, s.VL_SERVICO, cps.DT_ATIVACAO, cps.DT_DESATIVACAO, 
                  dici.ID_TIPO_MEIO_ACESSO, dici.ID_TIPO_TECNOLOGIA, dici.ID_TIPO_PRODUTO, 
                  tmeio.TX_DESCRICAO_TIPO, ttec.TX_DESCRICAO_TIPO, tprod.TX_DESCRICAO_TIPO, uname.TX_USERNAME,
                  bloq.ID_CONTRATO as bloq_id,
                  cps.ID_CONTRATO,cps.ID_PACOTE_SERVICO, cps.ID_SERVICO, 
                  ss.NM_SERVICO,uname.dt_desativacao
          FROM 
                  Contrato_PacoteServico_Servico as cps 
                  INNER JOIN PacoteServico as ps on (cps.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO) 
                  INNER JOIN PacoteServico_Servico as s on (cps.ID_SERVICO=s.ID_SERVICO) 
                  INNER JOIN Servico as ss on (ss.ID_SERVICO=cps.ID_SERVICO)
                  LEFT JOIN Servico_DICI as dici on (dici.ID_SERVICO=cps.ID_SERVICO)
                  LEFT JOIN TiposDiversos as tmeio on (tmeio.ID_TIPO_DIVERSOS=dici.ID_TIPO_MEIO_ACESSO) 
                  LEFT JOIN TiposDiversos as ttec on (ttec.ID_TIPO_DIVERSOS=dici.ID_TIPO_TECNOLOGIA) 
                  LEFT JOIN TiposDiversos as tprod on (tprod.ID_TIPO_DIVERSOS=dici.ID_TIPO_PRODUTO) 
                  LEFT JOIN UserName as uname on (cps.ID_CONTRATO=uname.ID_CONTRATO)
                  LEFT JOIN CONTRATOS_BLOQUEADOS as bloq on (bloq.ID_CONTRATO=cps.ID_CONTRATO)
          WHERE 
                cps.ID_CONTRATO={param_id_contrato} and
                ss.NM_SERVICO like '%SCM'
          ORDER BY
                    DT_ATIVACAO DESC 
                                  
                         """.format(param_id_contrato=id_contract))
        row = cursor.fetchone()
        if not row or row[5]: #se tem data de desativação, não vale
            res = ContractData(id_contract=id_contract, found=False)
            return res
        else:
            name = row[0]
            dl = row[1]
            ul =row[2]
            is_radio = row[9]=="radio" # a outra opção no wgc é "fibra"
            is_ftth = "ftth" in str(row[10]).lower()
            userName = row[12]
            bloqId = row[13]
            isBlocked = bloqId is not None

            home_access_type = "smartolt" if is_ftth else "aircontrol" if is_radio else "none"

            res = ContractData(id_contract=id_contract, download_speed=dl, upload_speed=ul, pack_name=name, is_radio=is_radio, is_ftth=is_ftth, found=True, user_name=userName, home_access_key=userName, home_access_type=home_access_type, bloqueado=isBlocked)

    with wdb.cursor() as cursor:
        cursor.execute("""
        SELECT  
                Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
                Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Cidade.TX_NOME_LOCALIDADE as cidade, 
                Endereco.TX_BAIRRO as bairro, UF.ID_UF as id_uf, UF.NM_UF as uf        
        FROM 
                Contrato as Contrato         
                INNER JOIN Endereco as Endereco on (Endereco.ID_ENDERECO=Contrato.ID_ENDERECO_INSTALACAO)
                LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
                LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
                LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)
        WHERE 
                ID_CONTRATO = {param_id_contrato}
    
                """.format(param_id_contrato=id_contract))
        row = cursor.fetchone()
        if row:
            logradouro: str = row[0]
            numero: str = row[1]
            complemento: str = row[2]
            cep: str = row[3]
            condominio: Optional[str] = row[4]
            cidade: Optional[str] = row[5]
            bairro: Optional[str] = row[6]
            uf: Optional[str] = row[7]
            medianetwork = "Rádio" if is_radio else "Cabo"
            endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, cep=cep, condominio=condominio, cidade=cidade, bairro=bairro, uf=uf, medianetwork=medianetwork)
            res.endereco = endereco

    print("getContract res = ", res)

    return res


          # SELECT
          #       NM_PACOTE_SERVICO,VL_DOWNLOAD, VL_UPLOAD, s.VL_SERVICO, DT_ATIVACAO, DT_DESATIVACAO
          # FROM
          #       ContratoItem as ci, PacoteServico as ps, PacoteServico_Servico as s, Contrato_PacoteServico_Servico as cps
          # WHERE
          #       ci.ID_CONTRATO={param_id_contrato} and
          #       ci.ID_PACOTE_SERVICO=ps.ID_PACOTE_SERVICO and
          #       ps.ID_PACOTE_SERVICO=s.ID_PACOTE_SERVICO and
          #       cps.ID_SERVICO=s.ID_SERVICO and
          #       cps.ID_CONTRATO=ci.ID_CONTRATO and
          #       s.VL_DOWNLOAD > 0
          # ORDER BY
          #           DT_ATIVACAO DESC