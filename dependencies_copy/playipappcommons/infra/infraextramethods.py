from typing import List

from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import getFieldNameByLevel, Endereco, buildFullImportName
from playipappcommons.infra.infraimportmethods import getInfraElementByFullImportName, importOrFindAddress
from playipappcommons.infra.inframethods import getInfraElementAddressHier, getInfraElement, isApproxAddr
from playipappcommons.infra.inframodels import InfraElement
from playipappcommons.ispbd.ispbddata import ContractData
from playipappcommons.playipchatmongo import getBotMongoDB
from playipappcommons.util.levenshtein import levenshteinDistanceDP






async def isApproxAddr2(enderecoCand:Endereco, enderecoCadastro:Endereco, threshold):
    fullName = buildFullImportName(enderecoCadastro)
    if not fullName:
        return False
    mdb = getBotMongoDB()
    #infraElement: InfraElement = await getInfraElementByFullImportName(mdb, fullName)
    infraElement: InfraElement = await importOrFindAddress(mdb, importResult=None, importExecUID=None, endereco=enderecoCadastro, doImport= False)
    if not infraElement:
        return 0.0
    return await isApproxAddr(enderecoCand, infraElement.id, threshold, enderecoCadastro)

class ContractMatchData:
    def __init__(self, contract:ContractData, prob_match:float):
        self.contract = contract
        self.prob_match = prob_match

    def __repr__(self):
        return "ContractMatchData(contract="+str(self.contract)+", prob_match="+str(self.prob_match)+")"

    def __str__(self):
        return self.__repr__()

    def __lt__(self, other):
        return self.prob_match < other.prob_match

    def __gt__(self, other):
        return self.prob_match > other.prob_match



async def selectCompatibleContracts(enderecoCand:Endereco, contratos:List[ContractData], threshold):
    enderecoCandSemLogradouro: Endereco = enderecoCand.copy()
    enderecoCandSemLogradouro.logradouro = None
    selected: List[ContractMatchData] = []
    for contrato in contratos:
        if not contrato.dt_cancelamento and contrato.endereco:
            contrato.endereco.prefix = "Comercial"
            print(contrato.endereco.cep)
            if enderecoCand.cep and contrato.endereco.cep.strip() == enderecoCand.cep.strip():
                #print("CEP match")
                prob_match_contrato = await isApproxAddr2(enderecoCandSemLogradouro, contrato.endereco, threshold)
            else:
                prob_match_contrato = await isApproxAddr2(enderecoCand, contrato.endereco, threshold)
            if prob_match_contrato > threshold:
                selected.append(ContractMatchData(contrato, prob_match_contrato))
    selected.sort(reverse=True)
    return selected

# contrato.endereco.cep.strip() == enderecoCand.logradouro.strip() or