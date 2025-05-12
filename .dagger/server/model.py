from sqlmodel import Field, SQLModel, Session, create_engine
from sqlmodel import Column, ForeignKey, UniqueConstraint
from ipaddress import IPv4Address, IPv6Address
from sqlalchemy.dialects.postgresql import INET
from pydantic import field_validator

g_engine = None

class SQLNoteEdit(SQLModel):
    title: str | None = None
    description: str | None = None


class SQLNoteCreate(SQLModel):
    title: str
    target: IPv4Address | IPv6Address
    description: str | None = None


class SQLTargetsTable(SQLModel, table=True):
    __tablename__ = "targets"
    ip_addr: str = Field(sa_column=Column(INET, primary_key=True))
    org: str | None = None

    @field_validator("ip_addr", mode="before")
    @classmethod
    def _transform_ip(cls, ip_addr: IPv4Address) -> str:
        return str(ip_addr)


class SQLServicesTable(SQLModel, table=True):
    __tablename__ = "services"
    target: str = Field(sa_column=Column(
            INET,
            ForeignKey("targets.ip_addr", ondelete="CASCADE"),
            primary_key=True
        ))
    port: int = Field(primary_key=True)
    name: str
    status: str

    @field_validator("target", mode="before")
    @classmethod
    def _transform_ip(cls, ip_addr: IPv4Address) -> str:
        return str(ip_addr)


class SQLFtpTable(SQLModel, table=True):
    __tablename__ = "ftp"
    target: str = Field(sa_column=Column(
            INET,
            ForeignKey("targets.ip_addr", ondelete="CASCADE"),
            primary_key=True
        ))
    is_anon: bool = True

    @field_validator("target", mode="before")
    @classmethod
    def _transform_ip(cls, ip_addr: IPv4Address) -> str:
        return str(ip_addr)

class SQLNoteTable(SQLModel, table=True):
    __tablename__ = "target_note"
    __table_args__ = (
        UniqueConstraint("target", "title", name="uniq_note_title"),
    )
    id: int | None = Field(default=None, primary_key=True)
    target: str = Field(sa_column=Column(
        INET,
        ForeignKey("targets.ip_addr", ondelete="CASCADE")
    ))
    title: str
    description: str | None = None
    
    @field_validator("target", mode="before")
    @classmethod
    def _transform_ip(cls, ip_addr: IPv4Address) -> str:
        return str(ip_addr)


def db_session(engine):
    def get_session():
        with Session(engine) as session:
            yield session
    return get_session


def db_engine():
    global g_engine
    if not g_engine:
        db_url = "postgresql://postgres:password@postgres:5432/shodan"
        g_engine = create_engine(db_url)

    return g_engine


def db_init():
    SQLModel.metadata.create_all(db_engine())
    
