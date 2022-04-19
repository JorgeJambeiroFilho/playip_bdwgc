import asyncio

from fastapi import APIRouter

from playip.bdwgc.import_analytics import importAllContratoPacoteServico
from playip.bdwgc.import_analytics_tickets import importAllContratoPacoteServicoTicket
from playipappcommons.analytics.analytics import getImportAnalyticDataResult
from playipappcommons.analytics.analyticsmodels import ImportAnalyticDataResult

importanalyticsrouter = APIRouter(prefix="/playipispbd/importanalytics")

@importanalyticsrouter.get("/importanalyticstickets", response_model=ImportAnalyticDataResult)
async def importAnalytics() -> ImportAnalyticDataResult:
    onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
    if not onGoingImportAnalyticDataResult.started:
        asyncio.create_task(importAllContratoPacoteServicoTicket())
    return onGoingImportAnalyticDataResult

@importanalyticsrouter.get("/getimportanalyticsticketsresult", response_model=ImportAnalyticDataResult)
async def getImportAnalyticsResult() -> ImportAnalyticDataResult:
    return await getImportAnalyticDataResult(False)


@importanalyticsrouter.get("/importanalytics", response_model=ImportAnalyticDataResult)
async def importAnalytics() -> ImportAnalyticDataResult:
    onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
    if not onGoingImportAnalyticDataResult.started:
        asyncio.create_task(importAllContratoPacoteServico())
    return onGoingImportAnalyticDataResult


@importanalyticsrouter.get("/getimportanalyticsresult", response_model=ImportAnalyticDataResult)
async def getImportAnalyticsResult() -> ImportAnalyticDataResult:
    return await getImportAnalyticDataResult(False)
