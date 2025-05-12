from fastapi import FastAPI, Depends, HTTPException, Query
from ipaddress import IPv4Address, IPv6Address
from sqlmodel import Session, select
from contextlib import asynccontextmanager
from typing import Annotated
from model import SQLTargetsTable, SQLNoteTable, SQLFtpTable, SQLNoteEdit, SQLNoteCreate
from model import db_session, db_engine, db_init
from sqlalchemy.exc import IntegrityError, OperationalError
import logging
import time

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    for _ in range(3):
        try:
            db_init()
            break
        except OperationalError as e:
            logging.warning(repr(e))
            time.sleep(3)
    yield


app = FastAPI(lifespan=lifespan)
PgSessionDep = Annotated[Session, Depends(db_session(db_engine()))]


@app.get("/ftp/anonymous")
async def get_ftp_anon(offset: int, 
                       limit: Annotated[int, Query(le=100)], 
                       session: PgSessionDep):
    stmt = select(SQLFtpTable). where(SQLFtpTable.is_anon == True)
    res = session.exec(stmt.offset(offset).limit(limit)).all()
    return res


@app.get("/target/info/{ip_addr}")
async def get_target_info(ip_addr: IPv4Address | IPv6Address, session: PgSessionDep):
    target = session.get(SQLTargetsTable, str(ip_addr))
    
    if not target:
        raise HTTPException(status_code=404, detail="does not exist")

    return target


@app.post("/target/note")
async def add_target_note(note_create: SQLNoteCreate, session: PgSessionDep):
    note = SQLNoteTable.model_validate(note_create)
    try:
        session.add(note)
        session.commit()
        session.refresh(note)
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail=repr(e.orig))
    return note.id


@app.get("/target/note/{id}")
async def get_target_note(id: int, session: PgSessionDep):
    note = session.get(SQLNoteTable, id)

    if not note:
        raise HTTPException(status_code=404)
    
    return note


@app.patch("/target/note/{id}")
async def upd_target_node(id: int, note_edit: SQLNoteEdit, session: PgSessionDep):
    note = session.get(SQLNoteTable, id)

    if not note:
        raise HTTPException(status_code=404)

    note_data = note_edit.model_dump(exclude_unset=True)
    note.sqlmodel_update(note_data)
    session.add(note)
    session.commit()
    session.refresh(note)

    return note_data


@app.delete("/target/note/{id}")
async def del_target_note(id: int, session: PgSessionDep):
    note = session.get(SQLNoteTable, id)

    if not note:
        raise HTTPException(status_code=404, detail="does not exist")
    
    session.delete(note)
    session.commit()

    return note


@app.get("/")
async def root():
    return {"message": "hello world!"}