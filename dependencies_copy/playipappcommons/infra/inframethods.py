import re
from typing import List

from bson import ObjectId

from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.inframodels import InfraElement, AddressQuery, AddressInFail
from playipappcommons.playipchatmongo import getBotMongoDB, getMongoClient


async def getChildren(mdb, pid:ObjectId) -> List[InfraElement]:

    #if pid is not None:
    cursor = mdb.infra.find({"parentId":pid})
    #else:
    #    cursor = mdb.infra.find({"parentId": {"$exists": False}})

    res = []
    for child in await cursor.to_list(500):
        res.append(InfraElement(**child))

    return res

async def getInfraRoot() -> InfraElement:
    return (await getInfraChildren())[0]

async def clearInfra():
    mdb = getBotMongoDB()
    await mdb.infra.delete_many({})
    return {"error": "ok"}


async def clearAllFails():
    mdb = getBotMongoDB()
    await mdb.infra.update_many({}, {'$set': {'inFail': False, "numDescendantsInFail":0}})
    return {"error": "ok"}


async def getInfraElements(ids: List[str]) -> List[InfraElement]:
    mdb = getBotMongoDB()
    elems: List[InfraElement] = []
    for id in ids:
        _id = ObjectId(id)
        json = await mdb.infra.find_one({"_id": _id})
        if json:
            elem = InfraElement(**json)
            elems.append(elem)
    return elems


async def getInfraElement(id:str=None) -> InfraElement:
    mdb = getBotMongoDB()
    _id = ObjectId(id)
    elem = InfraElement(**await mdb.infra.find_one({"_id": _id}))
    return elem

async def getInfraChildren(parentid:str=None) -> List[InfraElement]:
    mdb = getBotMongoDB()

    if await mdb.infra.count_documents({}) == 0:
        root: InfraElement = InfraElement(name="root")
        await mdb.infra.insert_one(root.dict())
        #await mdb.infra.insert_one({"name": "root", "inFail":False, "numDescendantsInFail": 0, "parentId": None})

    if parentid is not None:
        pid = ObjectId(parentid)
    else:
        pid = None

    return await getChildren(mdb, pid)

#@router.post("/playipchathelper/infra/getinfra")
#async def getInfra(body: dict):
#    mdb = getBotMongoDB()
#    res = await mdb.singletons.replace_one({"type":"infra"}, body, upsert=True)
#    return { "error":"ok" }

async def getInfraElementFailState(id:FAMongoId) -> AddressInFail:
    mdb = getBotMongoDB()
    _id = ObjectId(id)

    res: AddressInFail = AddressInFail(located=True, inFail=False)
    while _id:
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}))
        if ascendant.inFail and (not res.inFail or ascendant.dtPrevisao > res.dtPrevisao):
            res = AddressInFail(located=True, inFail=True, dtInterrupcao=ascendant.dtInterrupcao, dtPrevisao=ascendant.dtPrevisao, descricao=ascendant.message)
        if not ascendant.parentId:
            break
        _id = ascendant.parentId
    return res


stripPattern = re.compile('[\W_]+')
def stripNonAlphaNum(s):
    return stripPattern.sub('', s)

async def isAddressInFailIntern(addressQuery: AddressQuery) -> AddressInFail:
    address = addressQuery.address
    if addressQuery.endereco:
        address = str(addressQuery.endereco)
    test = addressQuery.test

    mdb = getBotMongoDB()
    ww = address.split()
    ww = [stripNonAlphaNum(w) for w in ww]
    if len(ww)==0:
        return AddressInFail(inFail=False, noFail=True)
    candidates = {}

    #     if len(ww)==1:

    rootl: List[InfraElement] = await getChildren(mdb, None)
    if len(rootl) !=1:
        return AddressInFail(inFail=False, noFail=False)
    root = rootl[0]
    if root.inFail:
        return AddressInFail(inFail=True, noFail=False)

    for i in range(len(ww)):
        cursor = mdb.infra.find({"filters.indexedWordPair":ww[i]})
        # for elem in await cursor.to_list(1000):
        async for elem in cursor:
            candidates[elem["_id"]] = InfraElement(**elem)

    if len(ww) > 1:
        for i in range(len(ww)-1):
            cursor = mdb.infra.find({"filters.indexedWordPair": ww[i] + ' ' + ww[i+1]})
            async for elem in cursor:
                candidates[elem["_id"]] = InfraElement(**elem)

    approved = []
    for cand in candidates.values():
        if cand.checkFilters(addressQuery):
            approved.append(cand)

    res: AddressInFail = AddressInFail(located=True, inFail=False)
    for appr in approved:
        apprFailState = await getInfraElementFailState(appr.id)
        if apprFailState.inFail and apprFailState.dtPrevisao > res.dtPrevisao:
            res = AddressInFail(located=True, inFail=True, dtInterrupcao=ascendant.dtInterrupcao, dtPrevisao=ascendant.dtPrevisao)

    return res

    # addrInFail = AddressInFail(inFail=inFail, noFail=noFail)
    # if test:
    #     addrInFail.elems = [ap.name for ap in approved]
    #
    # return addrInFail

async def adjustInfraElement(infraElement: InfraElement):
    mdb = getBotMongoDB()

    oldElem = InfraElement(**await mdb.infra.find_one({"_id": infraElement.id}))
    infraElement.numDescendantsInFail = oldElem.numDescendantsInFail
    infraElement.parentId = oldElem.parentId
    infraElement.inFail = oldElem.inFail
    infraElement.adjustIndexedWordPairs()
    elemDict = infraElement.dict(by_alias=True)
    await mdb.infra.replace_one({"_id": infraElement.id}, elemDict)

    return {"error": "ok"}

async def setInfraElementFailState(id:str, inFail:bool):
    mdb = getBotMongoDB()
    mdbcli = getMongoClient()

    async with await mdbcli.start_session() as s:
        async with s.start_transaction():

            _id = ObjectId(id)
            elem = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
            if inFail != elem.inFail:
                await mdb.infra.update_one({"_id": _id}, {'$set': {'inFail': inFail}},session=s)
                inc = 1 if inFail else -1
                while _id:
                    ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
                    ascendant.numDescendantsInFail += inc
                    await mdb.infra.update_one({"_id": _id}, {'$set': {'numDescendantsInFail': ascendant.numDescendantsInFail}}, session=s)
                    if not ascendant.parentId:
                        break
                    _id = ascendant.parentId

    return {"error": "ok"}


async def setInfraElementFailStateAndAdjustInfraElement(infraElement: InfraElement):
    mdb = getBotMongoDB()
    mdbcli = getMongoClient()

    async with await mdbcli.start_session() as s:
        async with s.start_transaction():

            oldElem = InfraElement(**await mdb.infra.find_one({"_id": infraElement.id}, session=s))

            oldInFail = oldElem.inFail
            inFail = infraElement.inFail
            _id = infraElement.id

            infraElement.numDescendantsInFail = oldElem.numDescendantsInFail
            infraElement.parentId = oldElem.parentId
            infraElement.adjustIndexedWordPairs()
            elemDict = infraElement.dict(by_alias=True)
            await mdb.infra.replace_one({"_id": infraElement.id}, elemDict, session=s)

            if inFail != oldInFail:
                inc = 1 if inFail else -1
                while _id:
                    #print ("AjustNumDescFail "+str(_id))
                    ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
                    ascendant.numDescendantsInFail += inc
                    await mdb.infra.update_one({"_id": _id}, {'$set': {'numDescendantsInFail': ascendant.numDescendantsInFail}}, session=s)
                    if not ascendant.parentId:
                        break
                    _id = ascendant.parentId

    return {"error": "ok"}


async def createInfraElement(parentid: str, addressparentid:str = None, name = "NoName"):
    mdb = getBotMongoDB()
    if addressparentid is None:
        addressparentid = parentid
    parent_id = ObjectId(parentid)
    parent_address_id = ObjectId(addressparentid)

    infraElement = InfraElement(parentId=parent_id, parentAddressId=parent_address_id, inFail=False, numDescendantsInFail=0, name=name)
    elemDict = infraElement.dict(by_alias=True)
    res = await mdb.infra.insert_one(elemDict)
    _id = res.inserted_id
    infraElement.id = _id
    #return {"error":"ok", "_id":str(_id), "parentId": parentId}
    return infraElement

async def moveTransactionCallback(session, elemId, toParentId):
    s = session
    if elemId == toParentId:
        return

    mdb = session.client.PlayIPChatHelper

    elem_id = ObjectId(elemId)
    cols = await mdb.list_collection_names()
    print(cols)
    ej = await mdb.infra.find_one({"_id": elem_id})
    elem = InfraElement(**ej)
    from_parent_id = elem.parentId
    to_parent_id = ObjectId(toParentId)

    mca = None

    ids = set()
    _id = from_parent_id
    while _id:
        ids.add(_id)
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
        if "_id" not in ascendant:
            break
        _id = ascendant.parent

    _id = to_parent_id
    while _id:
        if _id in ids:
            mca = _id
            break
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
        if "_id" not in ascendant:
            break
        _id = ascendant.parent

    _id = from_parent_id
    while _id:
        if _id == mca:
            break
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
        ascendant.numDescendantsInFail -= elem.numDescendantsInFail
        await mdb.infra.update_one({"_id": _id}, {'$set': {'numDescendantsInFail': ascendant.numDescendantsInFail}},
                                   session=s)
        if "_id" not in ascendant:
            break
        _id = ascendant.parent

    _id = to_parent_id
    while _id:
        if _id == mca:
            break
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
        ascendant.numDescendantsInFail += elem.numDescendantsInFail
        await mdb.infra.update_one({"_id": _id}, {'$set': {'numDescendantsInFail': ascendant.numDescendantsInFail}},
                                   session=s)
        if "_id" not in ascendant:
            break
        _id = ascendant.parent

    await mdb.infra.update_one({"_id": elem_id}, {'$set': {'parentId': to_parent_id, 'manuallyMoved':True}}, session=s)


async def moveInfraElement(elemId: str, toParentId: str):
    mdb = getBotMongoDB()
    mdbcli = getMongoClient()

    async with await mdbcli.start_session() as session:
        await session.with_transaction(lambda s: moveTransactionCallback(s, elemId, toParentId))

    return {"error":"ok", "id":elemId}



async def remove(elem_id:ObjectId):
    mdb = getBotMongoDB()
    children = await getChildren(mdb, elem_id)
    for child in children:
        await remove(child["_id"])
    mdb.infra.delete_one({"_id":elem_id})


async def removeInfraElement(elemId: str):
    mdb = getBotMongoDB()
    mdbcli = getMongoClient()

    async with await mdbcli.start_session() as s:
        async with s.start_transaction():

            elem_id = ObjectId(elemId)
            elemJSON = await mdb.infra.find_one({"_id": elem_id})
            if not elemJSON:
                return {"error": "NotFound", "_id": elemId}
            elem = InfraElement(**elemJSON)
            from_parent_id = elem.parentId

            _id = from_parent_id
            while _id:
                ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}, session=s))
                ascendant.numDescendantsInFail -= elem.numDescendantsInFail
                mdb.infra.update_one({"_id": _id}, {'$set': {'numDescendantsInFail': ascendant.numDescendantsInFail}}, session=s)
                if "_id" not in ascendant:
                       break
                _id = ascendant.parentId

            await remove(elem_id)

    return {"error":"ok", "_id": elemId}
