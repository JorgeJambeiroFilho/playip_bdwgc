import asyncio

from fastapi import APIRouter, Depends

from playip.bdwgc.import_addr import getImportAddressResultIntern, importAddressesIntern
from playipappcommons.auth.oauth2FastAPI import infrapermissiondep
from playipappcommons.infra.infraimportmethods import ImportAddressResult, ProcessAddressResult
from playipappcommons.playipchatmongo import getBotMongoDB

importrouter = APIRouter(prefix="/playipispbd/import")

@importrouter.get("/importaddresses", response_model=ImportAddressResult)
async def importAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingImportAddressResult: ImportAddressResult = await getImportAddressResultIntern(mdb, True)
    if onGoingImportAddressResult.hasJustStarted():
        asyncio.create_task(importAddressesIntern(mdb, onGoingImportAddressResult))
    return onGoingImportAddressResult

@importrouter.get("/getimportaddressesresult", response_model=ProcessAddressResult)
async def getImportAddressesResult(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    return await getImportAddressResultIntern(mdb, False)

@importrouter.get("/stopimportddresses", response_model=ImportAddressResult)
async def stopImportAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingIar: ImportAddressResult = await getImportAddressResultIntern(mdb, False)
    onGoingIar.abort()
    await onGoingIar.saveSoftly(mdb)
    return onGoingIar


@importrouter.get("/clearimportaddresses", response_model=ImportAddressResult)
async def clearImportAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingIar = ImportAddressResult()
    await onGoingIar.saveSoftly(mdb)
    return onGoingIar
