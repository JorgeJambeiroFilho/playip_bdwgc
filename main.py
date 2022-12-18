#teste git
import os

import uvicorn
from dynaconf import settings
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import traceback

from starlette.requests import Request
from starlette.responses import HTMLResponse

from playip.bdwgc.bdwgc import wgcrouter
from playip.bdwgc.import_addr import importrouter
from playip.bdwgc.import_contracts_router import importanalyticsrouter

app = FastAPI()

app.include_router(wgcrouter)
app.include_router(importrouter)
app.include_router(importanalyticsrouter)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("ENV_FOR_DYNACONF " + settings.ENV_FOR_DYNACONF)


#use na linha de comando
#uvicorn main:app --reload
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8016)

