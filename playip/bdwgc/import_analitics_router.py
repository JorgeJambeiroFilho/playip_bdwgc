import asyncio

from fastapi import APIRouter, Depends

from playip.bdwgc.import_analytics import importAllContratoPacoteServico
from playip.bdwgc.import_analytics_tickets import importAllContratoPacoteServicoTicket
from playipappcommons.analytics.analytics import getImportAnalyticDataResult
from playipappcommons.analytics.analyticsmodels import ImportAnalyticDataResult
from playipappcommons.auth.oauth2FastAPI import analyticspermissiondep

importanalyticsrouter = APIRouter(prefix="/playipispbd/importanalytics")

@importanalyticsrouter.get("/importanalyticstickets", response_model=ImportAnalyticDataResult)
async def importAnalytics(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
    onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
    if not onGoingImportAnalyticDataResult.started:
        asyncio.create_task(importAllContratoPacoteServicoTicket())
    return onGoingImportAnalyticDataResult

@importanalyticsrouter.get("/getimportanalyticsticketsresult", response_model=ImportAnalyticDataResult)
async def getImportAnalyticsResult(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
    return await getImportAnalyticDataResult(False)


@importanalyticsrouter.get("/importanalytics", response_model=ImportAnalyticDataResult)
async def importAnalytics(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
    onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
    if not onGoingImportAnalyticDataResult.started:
        asyncio.create_task(importAllContratoPacoteServico())
    return onGoingImportAnalyticDataResult


@importanalyticsrouter.get("/getimportanalyticsresult", response_model=ImportAnalyticDataResult)
async def getImportAnalyticsResult(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
    return await getImportAnalyticDataResult(False)
