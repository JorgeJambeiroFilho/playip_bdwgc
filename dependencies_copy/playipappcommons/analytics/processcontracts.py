from typing import cast

from playipappcommons.analytics.contractsanalytics import LRUCacheAnalytics, count_events_contract
from playipappcommons.analytics.contractsanalyticsmodels import ProcessAnalyticDataResult, ContractStorageAnalyticData
from playipappcommons.basictaskcontrolstructure import getControlStructure
from playipappcommons.playipchatmongo import getBotMongoDB

pctr_key = "ProcessContractsResult"


async def getProcessAnalyticDataResultIntern(mdb, begin:bool) -> ProcessAnalyticDataResult:
    return cast(ProcessAnalyticDataResult, await getControlStructure(mdb, pctr_key, begin))

async def process_contracts(res: ProcessAnalyticDataResult):
    mdb = getBotMongoDB()
    cache: LRUCacheAnalytics = LRUCacheAnalytics(mdb, "ISPContextMetrics", res, 10000)
    cursor = mdb.ContractData.find({})
    async for contractDataJson in cursor:
        contractData: ContractStorageAnalyticData = ContractStorageAnalyticData(contractDataJson)
        if contractData.not_accounted:
            if contractData.accounted:
                if not contractData.not_accounted.__eq__(contractData.accounted):
                    await count_events_contract(contractData.accounted, cache, True)
                    await count_events_contract(contractData.not_accounted, cache, False)
            else:
                await count_events_contract(contractData.not_accounted, cache, False)
            contractData.accounted = contractData.not_accounted
            contractData.not_accounted = None
            contract_json2 = contractData.dict(by_alias=True)
            await mdb.ContractData.replace_one({"id_contract": contractData.id_contract}, contract_json2, upsert=True)
            if await res.saveSoftly(mdb):
                return
    await res.saveHardly(mdb)

