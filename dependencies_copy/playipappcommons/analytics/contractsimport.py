import traceback

from playipappcommons.analytics.contractsanalyticsmodels import ImportAnalyticDataResult, ContractStorageAnalyticData, \
    ServicePackAndContractAnalyticData, ContractAnalyticData, ServicePackAnalyticData
from playipappcommons.playipchatmongo import getBotMongoDB
from datetime import datetime
from typing import AsyncGenerator, Optional

DRY = False

async def import_contract(last_contract, res: ImportAnalyticDataResult):
    mdb = getBotMongoDB()
    if last_contract.id_contract == "10338":
        print("contractsimport break ", last_contract.id_contract)
    contract_json = await mdb.ContractData.find_one({"id_contract":last_contract.id_contract})
    if contract_json:
        contractData: ContractStorageAnalyticData = ContractStorageAnalyticData(**contract_json)
    else:
        contractData: ContractStorageAnalyticData = ContractStorageAnalyticData(id_contract=last_contract.id_contract)
        res.num_new += 1

    res.num_processed += 1
    contractData.not_accounted = last_contract
    contract_json2 = contractData.dict(by_alias=True)

    await mdb.ContractData.replace_one({"id_contract":last_contract.id_contract}, contract_json2, upsert=True)




async def import_contracts_raw(it: AsyncGenerator[ServicePackAndContractAnalyticData, None], res: ImportAnalyticDataResult):
    fail = False
    mdb = getBotMongoDB()
    dts = datetime.now().strftime("_%Y_%m_%d_%H_%M_%S")
    print("import_contracts_raw")
    res.clearCounts()
    res.saveSoftly(mdb)
    #tabname = "ISPContextMetricsImp" + dts
    try:
        #createIndex_analytics(mdb, tabname)
        #cache: LRUCacheAnalytics = LRUCacheAnalytics(mdb, tabname, res, 10000)
        # if not DRY:
        #     await mdb.ISPContextMetrics.delete_many({})
        last_contract: Optional[ContractAnalyticData] = None
        async for spc in it:
            # endereco = spc.contract.endereco
            # if endereco.uf is None or endereco.cidade is None or endereco.bairro is None or endereco.logradouro is None:
            #     continue

            # element: Optional[InfraElement] = await findAddress(mdb, endereco)
            # if element is None:
            #     res.num_enderecos_nao_reconhecidos += 1
            #     continue

            if not last_contract or last_contract.id_contract != spc.contract.id_contract:
                if last_contract:
                    await import_contract(last_contract, res)
                    if await res.saveSoftly(mdb):
                        return
                    # if res.num_processed % 10 == 0:
                    #     await setImportAnalyticDataResult(res)
                    print(res)
                last_contract = spc.contract.copy(deep=True)
                # last_contract.fullStructName = await getFullStructuralName(element)
                # last_contract.fullAddressName = await getFullAddressName(element)

            if spc.ticket is None or\
               not last_contract.services or\
               last_contract.services[-1].fullName != spc.service.fullName or\
               last_contract.services[-1].DT_ATIVACAO != spc.service.DT_ATIVACAO:

                new_service: ServicePackAnalyticData = spc.service.copy(deep=True)

                if not new_service.DT_DESATIVACAO and last_contract.DT_CANCELAMENTO:
                    new_service.DT_DESATIVACAO = last_contract.DT_CANCELAMENTO

                if new_service.download_speed is None:
                    new_service.download_speed = 0
                    new_service.upload_speed = 0

                last_contract.services.append(new_service)

            if spc.ticket:
                last_contract.services[-1].tickets.append(spc.ticket)

        if last_contract:
            await import_contract(last_contract, res)
            print(res)
            await res.saveHardly(mdb)
            #await setImportAnalyticDataResult(res)



    except:
        traceback.print_exc()
        fail = True
    finally:
        await it.aclose()
        #await cache.close()
        #if not fail and not DRY:
        #    mdb[tabname].rename("ISPContextMetrics", dropTarget=True)
        res.complete = True
        await res.saveHardly(mdb)
        #await setImportAnalyticDataResult(res)
        print(res)

