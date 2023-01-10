from typing import cast

from playipappcommons.analytics.contractsanalytics import LRUCacheAnalytics, count_events_contract
from playipappcommons.analytics.contractsanalyticsmodels import ProcessContractsResult, ContractStorageAnalyticData
from playipappcommons.basictaskcontrolstructure import getControlStructure
from playipappcommons.playipchatmongo import getBotMongoDB

pctr_key = "ProcessContractsResult"


async def getProcessAnalyticDataResultIntern(mdb, begin:bool) -> ProcessContractsResult:
    return cast(ProcessContractsResult, await getControlStructure(mdb, pctr_key, begin))

async def clear_process_contracts(mdb, onGoingPcr: ProcessContractsResult):
    if await onGoingPcr.saveSoftly(mdb):
         await mdb.ContractData.update_many\
         (
             {"not_accounted": None},
             [
                 {"$set": {"not_accounted": "$accounted"}},
                 {"$set": {"accounted": None}},
             ]
         )
         await mdb.ContractData.update_many\
         (
             {},
             [
                 {"$unset": "ContractData"}
             ]
         )

         mdb.ISPContextMetrics.drop()
    else:
        onGoingPcr.fail = True
        onGoingPcr.message = "Limpeza não pode ocorrer durante processamento"

    onGoingPcr.done()
    await onGoingPcr.saveHardly(mdb)



async def process_contracts(mdb, res: ProcessContractsResult):
    cache: LRUCacheAnalytics = LRUCacheAnalytics(mdb, "ISPContextMetrics", res, 100000)
    try:
        cursor = mdb.ContractData.find({}, no_cursor_timeout=True)
        cursor.batch_size(1)
        try:
            async for contractDataJson in cursor:
                contractData: ContractStorageAnalyticData = ContractStorageAnalyticData(**contractDataJson)
                if contractData.id_contract == "10338":
                    print("process_contracts break ", contractData.id_contract)
                if contractData.not_accounted:
                    if contractData.accounted:
                        if not contractData.not_accounted.__eq__(contractData.accounted):
                            a = contractData.not_accounted.__eq__(contractData.accounted)
                            await count_events_contract(contractData.accounted, cache, True)
                            await count_events_contract(contractData.not_accounted, cache, False)
                    else:
                        await count_events_contract(contractData.not_accounted, cache, False)
                    contractData.accounted = contractData.not_accounted
                    contractData.not_accounted = None
                    contract_json2 = contractData.dict(by_alias=True)
                    await mdb.ContractData.replace_one({"id_contract": contractData.id_contract}, contract_json2, upsert=True)
                    res.num_processed += 1
                    if await res.saveSoftly(mdb):
                        print("Process contracts aborted")
                        return
                    #if res.num_processed >= 1:
                    #    break
        finally:
            cursor.close()
    finally:
        await cache.close()
        res.done()
        await res.saveHardly(mdb)
        print("Process contracts done")


# esse método não deve ser usado quando outros puderem ser chamados em paralelo.
# Serve ara testes offline, não para ser chamado via fastApi
async def clear_and_process_contracts() -> ProcessContractsResult:
    mdb = getBotMongoDB()
    onGoingPcr = ProcessContractsResult()
    await clear_process_contracts(mdb, onGoingPcr)
    onGoingPcr = await getProcessAnalyticDataResultIntern(mdb, True)
    await process_contracts(mdb, onGoingPcr)
    return onGoingPcr

