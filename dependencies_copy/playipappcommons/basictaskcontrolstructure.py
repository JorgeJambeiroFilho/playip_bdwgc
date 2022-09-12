import pydantic


class BasicTaskControlStructure(pydantic.BaseModel):
    fail: bool = False
    complete: bool = False
    started: bool = False
    message: str = "ok"
    aborted: bool = False
    num_processed: int = 0
    num_fails: int = 0
