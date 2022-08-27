import asyncio
import math
import re
from typing import List, Tuple, Optional

from bson import ObjectId

from playipappcommons.analytics.LRUCacheWordCount import LRUCacheWordCount, CountWordsResult, WordFreq
from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import getFieldNameByLevel, Endereco, address_level_fields
from playipappcommons.infra.inframodels import InfraElement, AddressQuery, AddressInFail
from playipappcommons.playipchatmongo import getBotMongoDB, getMongoClient
from playipappcommons.util.levenshtein import levenshteinDistanceDP


async def getChildren(mdb, pid:ObjectId) -> List[InfraElement]:

    #if pid is not None:
    cursor = mdb.infra.find({"parentId":pid})
    #else:
    #    cursor = mdb.infra.find({"parentId": {"$exists": False}})

    res = []
    async for child in cursor:
        res.append(InfraElement(**child))

    return res

#retorna liste de pares de (ids, nomes) de descendentes com nível de expansão entre minLevel emaxLevel
#se minLevel não é especificado, é igual a maxLevel
async def expandDescendants(mdb, pid:str, maxLevel:int, minLevel:int =-1, prefix:str="") -> List[Tuple[str, str]]:

    if minLevel == -1: minLevel = maxLevel
    cursor = mdb.infra.find({"parentId":ObjectId(pid)},{"_id":1, "name":1})
    res: List[(str,str)] = []

    if minLevel == 0:
        res.append((str(pid), prefix))


    if maxLevel >= 1:
        children: List[(str, str)] = []
        async for child in cursor:
            children.append((str(child["_id"]), child["name"].replace("/", "-")))

        for child in children:
            np = prefix + "/" + child[1] if prefix else child[1]
            descendants_child = await expandDescendants(mdb, child[0], maxLevel=maxLevel-1, minLevel=minLevel-1, prefix=np)
            res.extend(descendants_child)
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

async def getInfraElementFullStructuralName(id:FAMongoId) -> str:
    mdb = getBotMongoDB()
    _id = ObjectId(id)

    lis: List[str] = []
    while _id:
        infraElementDict = await mdb.infra.find_one({"_id": _id})
        if infraElementDict is None:
            raise Exception("EID não encontrado "+str(_id))
        ascendant = InfraElement(**infraElementDict)
        lis.append(ascendant.name)
        if not ascendant.parentId:
            break
        _id = ascendant.parentId
    lis.reverse()
    return "/".join(lis)

async def getInfraElementFullAddressName(id:FAMongoId) -> str:
    mdb = getBotMongoDB()
    _id = ObjectId(id)

    lis: List[str] = []
    while _id:
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}))
        lis.append(ascendant.name)
        if not ascendant.parentAddressId:
            break
        _id = ascendant.parentAddressId
    lis.reverse()
    return "/".join(lis)

async def getInfraElementAddressHier(id:FAMongoId) -> List[FAMongoId]:
    mdb = getBotMongoDB()
    _id = id #ObjectId(id)
    lis: List[FAMongoId] = []
    while _id:
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}))
        lis.append(ascendant.id)
        if not ascendant.parentAddressId:
            break
        _id = ascendant.parentAddressId
    lis.reverse()
    return lis



async def getInfraElementStructuralHier(id:FAMongoId) -> List[FAMongoId]:
    mdb = getBotMongoDB()
    _id = id #ObjectId(id)
    lis: List[FAMongoId] = []
    while _id:
        ascendant = InfraElement(**await mdb.infra.find_one({"_id": _id}))
        lis.append(ascendant.id)
        if not ascendant.parentId:
            break
        _id = ascendant.parentId
    lis.reverse()
    return lis


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
            res = AddressInFail(located=True, inFail=True, dtInterrupcao=appr.dtInterrupcao, dtPrevisao=appr.dtPrevisao)

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

def calc_pmatch(nome, n):
    if len(nome) == 0 and len(n) == 0:
        return 1
    d = levenshteinDistanceDP(nome, n)
    return 1 - d / max(len(nome), len(n))


# def findApprox(nome, subs):
#
#     ibest = -1
#     best_pmatch = 0
#     for i in range(len(subs)):
#         sub = subs[i]
#         for n in sub.nomes.keys():
#             pmatch = calc_pmatch(nome, n)
#             if pmatch > best_pmatch:
#                 best_pmatch = pmatch
#                 ibest = i
#     if best_pmatch > 0.95:
#         return ibest
#     else:
#         return -1

def findApprox(nome:str, subs: List[InfraElement], nivel: int):

    fieldName = getFieldNameByLevel(nivel)
    useApprox = fieldName == "logradouro" or fieldName == "bairro"
    lnome = nome.lower()
    best:InfraElement = None
    best_pmatch = 0
    for sub in subs:
        for fn in sub.addressLevelValues:
            n = fn.split("/")[-1]
            if useApprox:
                pmatch = calc_pmatch(lnome, n.lower())
            else:
                pmatch = 1.0 if lnome == n.lower() else 0.0

            if pmatch > best_pmatch:
                best_pmatch = pmatch
                best = sub
    if best_pmatch > 0.90:
        return best
    else:
        return None


wordCountCache = None

async def probWordIncludingWrongFields(cache, campoCorreto:str, word:str):

    sum  = 0.0
    for campo in address_level_fields:
        p = await probWord(cache, campo, word)
        if campo != campoCorreto:
            p *= 1.0/20
        sum = sum + p - sum * p #probabilidade do OR
    return sum

async def probWord(cache, campo:str, word:str):

    wfa: WordFreq = await cache.getByWord("ref", "global", "", campo, None)
    wf: WordFreq = await cache.getByWord("target", "global", "", campo, word)
    wfau: WordFreq = await cache.getByWord("ref_unique", "global", "", campo, None)

    S = 10.0
    pnew = wfau.freq / wfa.freq if wfa.freq else 1.0
    pw_given_new = 1.0 / 10000  # uma estimativa grosseira
    pw_given_not_new = (wf.freq + S / wfau.freq) / (wfa.freq + S) if wf.freq else 0.0

    p = pnew * pw_given_new + (1-pnew) * pw_given_not_new


    return p


def log_prob_wcli_given_wcad(s_cli:str, s_cad:str):
    d = levenshteinDistanceDP(s_cli, s_cad, cost_replace=math.log(10)+math.log(20), cost_del=math.log(10), cost_ins=math.log(10)+math.log(20))
    return -d


class Match:
    def __init__(self):
       self.other_word: Optional[str] = None
       self.log_prob_cli_given_cad: float = -math.inf

    def __repr__(self):
        return ""+str(self.other_word) + " lp=" + str(self.log_prob_cli_given_cad) + " p=" + str(math.exp(self.log_prob_cli_given_cad))


stopWords = { "de", "do", "dos", "da", "das"}
class MissCost:
   def __init__(self, word, log_prob, positionInString):
       self.position = -1 # posicao na ranking de palavras mais raras do string. A mais rara do string sempre fica com zero.
       self.postionInString = positionInString
       self.log_prob = log_prob
       self.word = word

   def __repr__(self):
       return self.word + " lp=" + str(self.log_prob) + " p=" + str(math.exp(self.log_prob))+ " pos=" + str(self.position)

   def __lt__(self, ot):
      return self.log_prob < ot.log_prob

   def log_prob_miss_cli(self):
       if self.word in stopWords:
           return math.log(0.5)
       elif self.postionInString > 3:
           log_prob_miss_cli = -math.log(10) + self.log_prob
       elif self.postionInString > 2:
           log_prob_miss_cli = -math.log(20) + self.log_prob
       elif self.position == 0:
           log_prob_miss_cli = -math.log(200) + self.log_prob # além de dar um miss a palavra teria que ter sido escrita como está pelo cliente
       else:
           log_prob_miss_cli = -math.log(50) + self.log_prob
       return log_prob_miss_cli

   def log_prob_miss_cad(self):
       if self.word in stopWords:
           return math.log(0.5)
       elif self.postionInString > 3:
           log_prob_miss_cad = -math.log(10)
       elif self.postionInString > 2:
           log_prob_miss_cad = -math.log(20)
       elif self.position == 0:
           log_prob_miss_cad = -math.log(200) # aqui é só o miss mesmo, lembre que o cálculo é da probabilidade do que o cliente digitou dado o qeu está no cadastro
       else:
           log_prob_miss_cad = -math.log(50)
       log_prob_miss_cad = max(log_prob_miss_cad, self.log_prob + math.log(10))
       log_prob_miss_cad = min(log_prob_miss_cad, math.log(1.0/10))
       return log_prob_miss_cad


async def isApproxFieldProb(cache, cli_value:str, cad_value:str, campo:str, threshold:float=0.9):
    lis_cli = [stripNonAlphaNum(s) for s in cli_value.lower().split()]
    lis_cad = [stripNonAlphaNum(s) for s in cad_value.lower().split()]
    rest_cli = set(lis_cli)
    #rest_cad = set(lis_cad)
    log_probs_cli = {}
    log_probs_cad = {}
    matches_cad = {}
    matches_cli = {}
    while rest_cli:
        w_cli = rest_cli.pop()
        if w_cli in matches_cli:
            raise Exception("Palavra já casada restando")
        match_cli: Match = Match()
        for w_cad in lis_cad:
            match_cad: Match = matches_cad.get(w_cad, None)
            pcc = log_prob_wcli_given_wcad(w_cli, w_cad)
            if pcc > match_cli.log_prob_cli_given_cad and (not match_cad or pcc > match_cad.log_prob_cli_given_cad):

                if match_cli.other_word:
                    matches_cad.pop(match_cli.other_word)

                match_cli.log_prob_cli_given_cad = pcc
                match_cli.other_word = w_cad
                if not match_cad:
                    match_cad: Match = Match()
                else:
                    matches_cli.pop(match_cad.other_word)
                    rest_cli.add(match_cad.other_word)
                    matches_cad.pop(w_cad)



                match_cad.other_word = w_cli
                match_cad.log_prob_cli_given_cad = pcc
                matches_cli[w_cli] = match_cli
                matches_cad[w_cad] = match_cad
                print(match_cli, " <-> ", match_cad)

    if len(matches_cli) != len(matches_cad):
        raise Exception("Número de matches incompatível")

    sum_match = 0.0
    sum_nomatch = 0.0

    for i in range(len(lis_cli)):
        w_cli = lis_cli[i]
        log_prob_cli = math.log(await probWordIncludingWrongFields(cache, campo, w_cli))
        log_probs_cli[w_cli] = MissCost(w_cli, log_prob_cli, i)
        sum_nomatch += log_prob_cli

    for i in range(len(lis_cad)):
        w_cad = lis_cad[i]
        log_prob_cad = math.log(await probWordIncludingWrongFields(cache, campo, w_cad))
        log_probs_cad[w_cad] = MissCost(w_cad, log_prob_cad, i)

    cli_possible_miss_costs = []
    cli_possible_miss_costs.extend(log_probs_cli.values())
    cad_possible_miss_costs = []
    cad_possible_miss_costs.extend(log_probs_cad.values())

    cli_possible_miss_costs.sort()
    cad_possible_miss_costs.sort()
    for i in range(len(cli_possible_miss_costs)):
        cli_possible_miss_costs[i].position = i
    for i in range(len(cad_possible_miss_costs)):
        cad_possible_miss_costs[i].position = i


    for w_cli, match_cli in matches_cli.items():
        match_cad: Match = matches_cad[match_cli.other_word]
        if match_cad.log_prob_cli_given_cad != match_cli.log_prob_cli_given_cad:
            raise Exception("matches incompatíveis")
        w_cad = match_cli.other_word
        cli_miss = log_probs_cli[w_cli]
        cad_miss = log_probs_cad[w_cad]
        log_prob_miss_cli = cli_miss.log_prob_miss_cli()
        log_prob_miss_cad = cad_miss.log_prob_miss_cad()
        log_prob_miss = log_prob_miss_cad + log_prob_miss_cli

        if log_prob_miss > match_cli.log_prob_cli_given_cad:
            sum_match += log_prob_miss
        else:
            sum_match += match_cli.log_prob_cli_given_cad

    for w_cli in lis_cli:
        if w_cli not in matches_cli:
            cli_miss = log_probs_cli[w_cli]
            log_prob_miss_cli = cli_miss.log_prob_miss_cli()
            sum_match += log_prob_miss_cli

    for w_cad in lis_cad:
        if w_cad not in matches_cad:
            cad_miss = log_probs_cad[w_cad]
            log_prob_miss_cad = cad_miss.log_prob_miss_cad()
            sum_match += log_prob_miss_cad

    log_ratio = sum_match - sum_nomatch
    ratio = math.exp(log_ratio)

    prob_is_match = ratio / (1.0+ratio)

    print(prob_is_match)

    return prob_is_match > threshold




#isApproxFieldProb(cache, cli_value:str, cad_value:str, campo:str, threshold:float=0.9)

async def isApproxFieldWithProbWhenNeeded(nome:str, ie: InfraElement, nivel: int, threshold:float=0.9):
    cache: LRUCacheWordCount = getWordCountCache()
    fieldName = getFieldNameByLevel(nivel)
    useApprox = fieldName == "logradouro" or fieldName == "bairro"
    lnome = nome.lower()
    best_pmatch = 0
    for fn in ie.addressLevelValues:
        n = fn.split("/")[-1]
        if useApprox:
            pmatch =  await isApproxFieldProb(cache, nome, n.lower(), fieldName, threshold)
            #pmatch = calc_pmatch(lnome, n.lower())
        else:
            pmatch = 1.0 if lnome == n.lower() else 0.0

        if pmatch > best_pmatch:
            best_pmatch = pmatch

    return best_pmatch > threshold


async def isApproxField(nome:str, ie: InfraElement, nivel: int, threshold:float=0.9):
    fieldName = getFieldNameByLevel(nivel)
    useApprox = fieldName == "logradouro" or fieldName == "bairro"
    lnome = nome.lower()
    best_pmatch = 0
    for fn in ie.addressLevelValues:
        n = fn.split("/")[-1]
        if useApprox:
            pmatch = calc_pmatch(lnome, n.lower())
        else:
            pmatch = 1.0 if lnome == n.lower() else 0.0

        if pmatch > best_pmatch:
            best_pmatch = pmatch

    return best_pmatch > threshold


async def isApproxAddr(endereco:Endereco, eid:FAMongoId, threshold):

    eids = await getInfraElementAddressHier(eid)
    for i in range(1, len(eids)):
        ie: InfraElement = await getInfraElement(str(eids[i]))
        nome = endereco.getFieldValueByLevel(i)
        if nome:
            ia = await isApproxFieldWithProbWhenNeeded(nome, ie, i, 0.9) #threshold
            if not ia:
                return False
    return True


wordCountCache: LRUCacheWordCount
def getWordCountCache():
    global wordCountCache
    if wordCountCache is None:
        mdb = getBotMongoDB()
        cwr:CountWordsResult = CountWordsResult()
        wordCountCache = LRUCacheWordCount(mdb, "StreetWordCount", cwr, 10000)
    return wordCountCache


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    #loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)

    #loop.run_until_complete(isApproxFieldProb(wordCountCache, "Preca Alfa Centouro", "Praça Alpha de Centauro (centro de apoio2)", "logradouro", 0.9))
    loop.run_until_complete(isApproxFieldProb(getWordCountCache(), "Centauro", "Praça Alpha de Centauro (centro de apoio2)", "logradouro", 0.9))
