import asyncio

from fastapi import APIRouter, Depends

from playip.bdwgc.import_contracts_tickets import importAllContratoPacoteServicoTicket, getImportContractsResultIntern
from playipappcommons.analytics.contractsanalyticsmodels import ImportContractsResult
from playipappcommons.auth.oauth2FastAPI import analyticspermissiondep
from playipappcommons.playipchatmongo import getBotMongoDB

importanalyticsrouter = APIRouter(prefix="/playipispbd/importcontracts")

@importanalyticsrouter.get("/importcontractswithtickets", response_model=ImportContractsResult)
async def importContracts(auth=Depends(analyticspermissiondep)) -> ImportContractsResult:

    mdb = getBotMongoDB()
    onGoingImportAnalyticsResult: ImportContractsResult = await getImportContractsResultIntern(mdb, True)
    if onGoingImportAnalyticsResult.hasJustStarted():
        asyncio.create_task(importAllContratoPacoteServicoTicket(mdb, onGoingImportAnalyticsResult))
    return onGoingImportAnalyticsResult



@importanalyticsrouter.get("/getimportcontractswithticketsresult", response_model=ImportContractsResult)
async def getImportContractsResult(auth=Depends(analyticspermissiondep)) -> ImportContractsResult:
    mdb = getBotMongoDB()
    return await getImportContractsResultIntern(mdb, False)


@importanalyticsrouter.get("/stopimportcontractswithtickets", response_model=ImportContractsResult)
async def stopImportAddresses(auth=Depends(analyticspermissiondep)) -> ImportContractsResult:
    mdb = getBotMongoDB()
    onGoingIar: ImportContractsResult = await getImportContractsResultIntern(mdb, False)
    onGoingIar.abort()
    await onGoingIar.saveSoftly(mdb)
    return onGoingIar


@importanalyticsrouter.get("/clearimportcontractswithtickets", response_model=ImportContractsResult)
async def clearImportAddresses(auth=Depends(analyticspermissiondep)) -> ImportContractsResult:
    mdb = getBotMongoDB()
    onGoingIar = ImportContractsResult()
    await onGoingIar.saveSoftly(mdb)
    return onGoingIar


# @importanalyticsrouter.get("/importanalyticstickets", response_model=ImportAnalyticDataResult)
# async def importAnalytics(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
#     onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
#     if not onGoingImportAnalyticDataResult.started:
#         asyncio.create_task(importAllContratoPacoteServicoTicket())
#     return onGoingImportAnalyticDataResult
#
# @importanalyticsrouter.get("/getimportanalyticsticketsresult", response_model=ImportAnalyticDataResult)
# async def getImportAnalyticsResult(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
#     return await getImportAnalyticDataResult(False)
#
#
# @importanalyticsrouter.get("/importanalytics", response_model=ImportAnalyticDataResult)
# async def importAnalytics(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
#     onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
#     if not onGoingImportAnalyticDataResult.started:
#         asyncio.create_task(importAllContratoPacoteServico())
#     return onGoingImportAnalyticDataResult
#
#
# @importanalyticsrouter.get("/getimportanalyticsresult", response_model=ImportAnalyticDataResult)
# async def getImportAnalyticsResult(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
#     return await getImportAnalyticDataResult(False)
