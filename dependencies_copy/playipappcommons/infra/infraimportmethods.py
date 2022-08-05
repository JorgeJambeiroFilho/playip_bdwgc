from typing import List, Optional

import pydantic
from bson import ObjectId

from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import Endereco, increase_address_level, getFieldNameByLevel, buildFullImportName
from playipappcommons.infra.inframethods import getChildren, createInfraElement, getInfraRoot, \
    getInfraElementAddressHier, getInfraElement, findApprox
from playipappcommons.infra.inframodels import InfraElement, AddressQuery, AddressInFail
from playipappcommons.playipchatmongo import getBotMongoDB
from playipappcommons.util.levenshtein import levenshteinDistanceDP


class ImportAddressResult(pydantic.BaseModel):
    fail: bool = False
    complete: bool = False
    message: str = "ok"
    num_bairros: int = 0
    num_cidades: int = 0
    num_condominios: int = 0
    num_logradouros: int = 0
    num_numeros: int = 0
    num_complementos: int = 0
    num_ufs: int = 0
    num_prefixs: int = 0 #o erro ortográfico tem que ser mantido por enquanto, pois há um acesso dinâmico que falharia

    num_alt_bairros: int = 0
    num_alt_cidades: int = 0
    num_alt_condominios: int = 0
    num_alt_logradouros: int = 0
    num_alt_numeros: int = 0
    num_alt_complementos: int = 0
    num_alt_ufs: int = 0
    num_alt_prefixs: int = 0

    last_id_endereco: int = 0

    num_processed: int = 0




async def getInfraElementByFullImportName(mdb, fullName:str) -> InfraElement:
    delem = await mdb.infra.find_one({"addressFullNames":fullName})
    if not delem:
        return None
    infraElement: InfraElement = InfraElement(**delem)
    return infraElement




async def getAddressChildren(mdb, pid:ObjectId) -> List[InfraElement]:

    #if pid is not None:
    cursor = mdb.infra.find({"parentAddressId":pid})
    #else:
    #    cursor = mdb.infra.find({"parentId": {"$exists": False}})

    res = []
    for child in await cursor.to_list(500):
        res.append(InfraElement(**child))

    return res


def findByRules(endereco: Endereco, candidates:List[InfraElement]) -> Optional[InfraElement]:
    for cand in candidates:
        aq: AddressQuery = AddressQuery(endereco=endereco)
        does_match = cand.checkFilters(aq)
        if does_match:
            return cand
    return None


# def buildEndereco(infraElement: InfraElement):
#
#     ss = infraElement.addressFullNames.split("/")
#     endereco:Endereco = Endereco()
#     for t in range(len(ss)):
#         endereco.setFieldValueByLevel(t+1, ss[t])
#
# async def repositionChildrenByRules(mdb, address_parent_id:str):
#
#     children: List[InfraElement] = await getAddressChildren(mdb, address_parent_id)
#     for child in children:
#         if child.addressLevelValues: # exclui o nós puramente de infra
#             endereco:Endereco = buildEndereco(child)
#             intermediateParent: InfraElement = findByRules(endereco, children)
#             while intermediateParent:
#                 ichildren: List[InfraElement] = await getAddressChildren(mdb, intermediateParent.id)
#                 intermediateParent: InfraElement = findByRules(endereco, children)
#             child.parentId = intermediateParent.id
#             elemDict = child.dict(by_alias=True)
#             await mdb.infra.replace_one({"_id": child.id}, elemDict)


async def findAddress(mdb, endereco: Endereco) -> Optional[InfraElement]:
    return await importOrFindAddress(mdb, None, None, endereco, doImport=False)


async def importOrFindAddress(mdb, importResult: Optional[ImportAddressResult], importExecUID:Optional[str], endereco: Endereco, nivel: int = -1, doImport: bool = True) -> Optional[InfraElement]:
    if endereco.uf is None or endereco.cidade is None or endereco.bairro is None or endereco.logradouro is None:
        return None
    if nivel == 0:
        return await getInfraRoot()

    fullName = buildFullImportName(endereco, nivel)
    infraElement: InfraElement = await getInfraElementByFullImportName(mdb, fullName)
    if infraElement and (infraElement.importExecUID == importExecUID or not doImport):
        return infraElement

    parent: InfraElement = await importOrFindAddress(mdb, importResult, importExecUID, endereco, increase_address_level(nivel), doImport=doImport)
    if parent is None:
        if not doImport:
            return None
        else:
            raise Exception("Nó pai não criado em infaestrutura")

    children: List[InfraElement] = await getAddressChildren(mdb, parent.id)
    sparent = parent
    intermediateParent: InfraElement = findByRules(endereco, children)
    while intermediateParent:
        sparent = intermediateParent
        children: List[InfraElement] = await getAddressChildren(mdb, intermediateParent.id)
        intermediateParent: InfraElement = findByRules(endereco, children)

    cname = endereco.getFieldValueByLevel(nivel)
    if not infraElement:
        infraElement:InfraElement = findApprox(cname, children, nivel)

    if not doImport:
        return infraElement

    if not infraElement:
        infraElement:InfraElement = await createInfraElement(str(parent.id), str(sparent.id), cname)
        infraElement.addressLevelValues.append(cname)
        infraElement.addressLevel = nivel
        infraElement.addressFullNames.append(fullName)
        infraElement.importExecUID = importExecUID
        elemDict = infraElement.dict(by_alias=True)
        await mdb.infra.replace_one({"_id": infraElement.id}, elemDict)
        rcname = "num_" + getFieldNameByLevel(nivel) + "s"
        if importResult:
            count = getattr(importResult, rcname)
            setattr(importResult,rcname, count + 1)
    else:
        changed = False
        infraElement.importExecUID = importExecUID
        if not infraElement.manuallyMoved:
            infraElement.parentId = sparent.id
        found = False
        for cn in infraElement.addressLevelValues:
            found = found or cn == cname
        changed = changed
        if not found:
            infraElement.addressLevelValues.append(cname)
            changed = True

        found = False
        for fn in infraElement.addressFullNames:
            found = found or fn == fullName
        if not found:
            infraElement.addressFullNames.append(fullName)
            changed = True

        if changed:
            rcname = "num_alt_" + getFieldNameByLevel(nivel) + "s"
            if importResult:
                count = getattr(importResult, rcname)
                setattr(importResult,rcname, count + 1)

        elemDict = infraElement.dict(by_alias=True)
        await mdb.infra.replace_one({"_id": infraElement.id}, elemDict)


    return infraElement


