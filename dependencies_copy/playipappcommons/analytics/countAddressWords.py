from typing import cast

import datetime
import os

import pymongo

from playipappcommons.analytics.LRUCacheWordCount import CountWordsResult, LRUCacheWordCount, WordFreq
from playipappcommons.basictaskcontrolstructure import getControlStructure
from playipappcommons.infra.endereco import getFieldLevelByName, getFieldNameByLevel
from playipappcommons.infra.inframethods import stripNonAlphaNum
from playipappcommons.infra.inframodels import InfraElement, InfraElementLevelName
from playipappcommons.playipchatmongo import getBotMongoDB

cwr_key = "CountWordsResult"

async def getCountWordsResultIntern(mdb, begin:bool) -> CountWordsResult:
    return cast(CountWordsResult, await getControlStructure(mdb, cwr_key, begin))

# async def setWordCountResult(mdb, adr: CountWordsResult):
#     resDict = adr.dict(by_alias=True)
#     resDict["key"] = "CountWordsResult"
#     await mdb.control.replace_one({"key": "CountWordsResult"}, resDict, upsert=True)


# async def cleanWordCount():
#     wcr = CountWordsResult()
#     await setWordCountResult(wcr)

async def clear_count_words(mdb, onGoingCWR: CountWordsResult) -> CountWordsResult:
    if onGoingCWR.isGoingOn():
        onGoingCWR.message = "CannotClearRunningProcess"
    else:
        onGoingCWR = CountWordsResult()
        if await onGoingCWR.saveSoftly(mdb):
            mdb.StreetWordCount.delete_many({})
    return onGoingCWR

async def cleanAndCountWords():
    mdb = getBotMongoDB()
    onGoingCWR: CountWordsResult = await getCountWordsResultIntern(mdb, False)
    await clear_count_words(mdb,onGoingCWR)
    await countWordsIntern(mdb,onGoingCWR)


DRY = False

async def countWordsOfElementIntern(ie: InfraElement, cache: LRUCacheWordCount):

    counted = set()
    for name in ie.addressLevelNames:
        loname = name.name.lower()
        # lonamec = re.sub(r'\W+', ' ', loname)
        # if loname!=lonamec:
        #     print("loname!=lonamec")

        words = [stripNonAlphaNum(s) for s in loname.split()]
        for word in words:
            if word not in counted:
                counted.add(word)
                wf: WordFreq = await cache.getByWord("target", "global", "", getFieldNameByLevel(ie.addressLevel), word)
                wf.count()
                wfa: WordFreq = await cache.getByWord("ref", "global", "", getFieldNameByLevel(ie.addressLevel), None)
                wfa.count()
                if wf.freq == 1:
                    wfau: WordFreq = await cache.getByWord("ref_unique", "global", "", getFieldNameByLevel(ie.addressLevel), None)
                    wfau.count()

                # if word == "centauro":
                #    print(name, wf.word, wf.freq, ie.id, ie.parentAddressId)
            # wfd = await mdb[tabname].find_one({"word":word})
            # await mdb[tabname].update({"word": word}, {"$set": {"freq": wfd["freq"]+1}})

BASE_DIR = os.getcwd() #os.path.dirname(os.path.abspath(__file__))

# mdb[tabname].create_index(
#     [
#         ("role", pymongo.ASCENDING),
#         ("context_type", pymongo.ASCENDING),
#         ("context_value", pymongo.ASCENDING),
#         ("target_context_type", pymongo.ASCENDING),
#         ("target_value", pymongo.ASCENDING)
#     ],
#     background=False, name="r_ct_cv_tct_tv"
# )
# print("Created Word Index")


def buildInfraElementFromCEPLine(lin) -> InfraElement:
    lis = lin.strip().split("\t")
    cep = lis[0]
    cidadeEstado = lis[1].split("/")
    cidade = cidadeEstado[0]
    estado = cidadeEstado[1]
    if len(lis) > 2:
        bairro = lis[2]
    else:
        bairro = ""
    if len(lis) > 3:
        logradouro = lis[3]
    else:
        logradouro = ""
    lvirg = logradouro.find(",")
    if lvirg >= 0:
        logradouro = logradouro[:lvirg]
    ltraco = logradouro.find(" - ")
    if ltraco >= 0:
        logradouro = logradouro[:ltraco]
    if len(lis) > 4:
        complemento = lis[4]
    else:
        complemento = ""

    ie: InfraElement = InfraElement(name=logradouro)
    ie.addressLevel = getFieldLevelByName("logradouro")
    ie.addressLevelNames.append(InfraElementLevelName(name=logradouro))
    return ie

async def countWordsIntern(mdb, onGoingWordCountResult: CountWordsResult):
    fail = False
    await onGoingWordCountResult.saveSoftly(mdb)
    #dts = datetime.datetime.now().strftime("_%Y_%m_%d_%H_%M_%S")
    print("countWords")
    tabname = "StreetWordCount" # + dts

    cache: LRUCacheWordCount = LRUCacheWordCount(mdb, tabname, onGoingWordCountResult, 10000)
    try:
        with open(BASE_DIR+"/data/ceps.txt") as fp:
            lin = fp.readline()
            pos = 0
            while(lin):
                ie: InfraElement = buildInfraElementFromCEPLine(lin)
                if pos > onGoingWordCountResult.position:
                    await countWordsOfElementIntern(ie, cache)
                    onGoingWordCountResult.num_processed += 1
                    onGoingWordCountResult.position = pos

                if pos % 100 == 0:
                    if await onGoingWordCountResult.saveSoftly(mdb):
                        return

                lin = fp.readline()
                pos += 1

        cursor = mdb.infra.find({"maxAddressLevelNameTimestamp": {"$gt": onGoingWordCountResult.timestamp}}).sort([("maxAddressLevelNameTimestamp", pymongo.ASCENDING)])
        async for ied in cursor:

            ie: InfraElement = InfraElement(**ied)
            onGoingWordCountResult.num_processed += 1
            onGoingWordCountResult.timestamp = ie.maxAddressLevelNameTimestamp
            await countWordsOfElementIntern(ie, cache)
            if onGoingWordCountResult.num_processed % 100 == 0:
                if await onGoingWordCountResult.saveSoftly(mdb):
                    return

    finally:
        await cache.close()
        # if not fail and not DRY:
        #     mdb[tabname].rename("StreetWordCount", dropTarget=True)
        onGoingWordCountResult.done()
        await onGoingWordCountResult.saveHardly(mdb)
        print(onGoingWordCountResult)
