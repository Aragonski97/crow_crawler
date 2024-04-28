from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import (create_async_engine,
                                    async_sessionmaker,
                                    AsyncSession,
                                    AsyncConnection,
                                    AsyncEngine
                                    )
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import URL, exc, text, inspect, select, update, delete, insert
from typing import AsyncIterator, Any
from collections import OrderedDict

from crow_config import CORE_DATABASE_PARAMS
from contextlib import asynccontextmanager
from abc import ABC, ABCMeta, abstractmethod

from app.logger.crow_logger import logger

from app.schemas.error_schema import ErrorSchema

from app.utils.decompile_url_name import get_url_name

from app.object_mappings import Base, BaseModel, resolve_model, orm_to_schema


def construct_engine_url(config: dict) -> URL:
    """
    :param config: a dict with which to create an adequate sqlalchemy engine URL.
    Currently used only in combination with crow_config CORE_DATABASE_PARAMS,
    but it's intended to provide users with ability to connect to a different database.
    Still not implemented, but will serve later on.
    """
    try:
        return URL.create(
            drivername=config["dialect"]+"+"+config["driver"],
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"])
    except KeyError as err:
        logger.error(err)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            ))


class Database(ABC):
    __metaclass__ = ABCMeta

    @abstractmethod
    def connect_engine(self) -> Any:
        ...

    @abstractmethod
    def get_session(self) -> AsyncSession:
        ...

    @abstractmethod
    def get_connection(self) -> AsyncConnection:
        ...

    @abstractmethod
    def close(self) -> bool:
        ...


class MySQLDatabase(Database):

    def __init__(self, engine_config: dict = {}):
        self._engine = self.connect_engine(pool_pre_ping=True, **engine_config)
        self._sessionmaker = async_sessionmaker(self._engine)
        print("Database Started")

    def connect_engine(self, **engine_config) -> AsyncEngine:
        """
        Create an SqlAlchemy AsyncEngine that will be used by the calls.
        """
        engine_url = construct_engine_url(config=CORE_DATABASE_PARAMS)
        try:
            engine = create_async_engine(engine_url, **engine_config)
        except SQLAlchemyError as err:
            logger.error(err)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=ErrorSchema(
                                    err_type=err,
                                    err_message=err.args[0],
                                ))

        return engine

    @asynccontextmanager
    async def get_connection(self) -> AsyncIterator[AsyncConnection]:
        """
        Async Context Manager generator for AsyncConnections.
        Should be used as follows:

        connection = CORE_DATABASE.get_connection():\n
        async with connection as conn:
            ...
        """
        if self._engine is None:
            logger.error("Core database not initialized.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=ErrorSchema(
                                    err_type=exc.SQLAlchemyError,
                                    err_message="Core database not initialized."
                                ))

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @asynccontextmanager
    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """
        Async Context Manager generator for AsyncConnections.
        Should be used as follows:

        session = CORE_DATABASE.get_session():\n
        async with session as sesh:
            ...
        """
        if self._sessionmaker is None:
            logger.error("Core database not initialized.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=ErrorSchema(
                                    err_type=exc.SQLAlchemyError,
                                    err_message="Core database not initialized."
                                ))
        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def close(self):
        """
        Disposes of Database engine and sends the instances to garbage collector.
        """
        if self._engine is None:
            logger.error("Core database not initialized.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=ErrorSchema(
                                    err_type=exc.SQLAlchemyError,
                                    err_message="Core database not initialized."
                                ))
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    async def create_core_tables(self):
        """
        Creates core tables for Profiles, Scrapers, Selectors and Users.
        """
        try:
            connection = self.get_connection()
            async with connection as conn:
                await conn.run_sync(Base.metadata.create_all)
            return
        except Exception as err:
            logger.error(err)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=ErrorSchema(
                                    err_type=err,
                                    err_message=err.args[0]
                                ))

    async def create_dynamic_table(self, table_name: str, selectors, replace: bool = False):
        """
        :param table_name: desired name for new table
        :param replace: if replace is True, replaces the table that already exists in database.
        Creates a table in the CORE_DATABASE (currently, since no custom dbs are implemented)
        based on name and selectors provided in params.
        Currently, it is specified to create all the values as VARCHAR(255), since I do the processing
        of data outside this program. Please reformat the code for your own needs.
        """
        try:
            async with self.get_connection() as connection:
                table_exists = await connection.run_sync(
                    lambda sync_conn: inspect(sync_conn).has_table(get_url_name(table_name))
                )
                if not table_exists:
                    statement = self.table_creator(
                        table_name=table_name,
                        selectors=selectors,
                        replace=replace
                    )
                    await connection.execute(text(statement))
                    print(f"Table for {table_name} created.")
                    return
                logger.error("Table exists!!!!!!")
        except Exception as err:
            logger.error(err)
            print(err)

    @staticmethod
    def table_creator(table_name: str, selectors, replace: bool = False) -> str:
        """
        Creates an SQL query string for table creation based on params.
        """
        # expected to be like www.123.test...
        _name = get_url_name(table_name)
        # static columns each row needs to contain. Add additional if needed be, but don't forget to
        # make sure the package data contains this as key-value pair. You can add them manually later
        # inside the insert_scraped_data function.
        columns = [
            "selector_id INTEGER PRIMARY KEY AUTO_INCREMENT",
        ]

        for selector in selectors:
            columns.append(f"`{selector.name}` VARCHAR(255) {'NOT NULL' if selector.required else ''}")
        # line below serves to maintain order in columns, can very well be placed inside columns' initiation.
        columns.append("scraped_date DATETIME DEFAULT CURRENT_TIMESTAMP")
        columns.append(f"UNIQUE (`foreign_id`)")
        if replace:
            query = f"CREATE OR REPLACE TABLE {_name} ({', '.join(columns)})"
        else:
            query = f"CREATE TABLE {_name} ({', '.join(columns)})"
        return query

    async def insert_scraped_data(self, data: list[dict], table_name: str):

        """
        :param data: intended to be a batch of scraped data
        ( usually 200. Find in app.class_models.step_model BATCH_LENGTH var)
        :param table_name: already created table in core database.
        Bulk inserts data into a dynamic table in SQL.
        """
        session = self.get_session()
        async with session as session:
            for item in data:
                try:
                    # you can add static key-value pairs to item if need be.
                    args_str = '(' + ','.join(f"'{v}'" for k, v in item.items()) + ')'
                    keys = '(' + ', '.join(f"`{k}`" for k, v in item.items()) + ')'
                    await session.execute(
                        text(
                            f"INSERT INTO {get_url_name(table_name)} {keys} VALUES " + args_str
                        )
                    )
                except Exception as err:
                    print(err)
                    logger.error(err)
            await session.commit()
        return

    async def insert_item(self, schema: BaseModel) -> Any:
        """
        :param schema: Schema that will be inserted.
        :return: Results returned by selecting from core database.
        """
        model = resolve_model(schema)
        session = self.get_session()
        try:
            async with session as session:
                statement = insert(model).values(schema.model_dump())
                await session.execute(statement)
                await session.commit()
                return schema
        except SQLAlchemyError as err:
            raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                detail=jsonable_encoder(ErrorSchema(
                                    err_type=err,
                                    err_message=err.orig.args[1]
                                )))

    async def delete_item(self, schema: BaseModel) -> Any:
        """
        :param schema: Schema that will be deleted.
        :return: Results returned by selecting from core database.
        """
        model = resolve_model(schema)
        session = self.get_session()
        async with session as session:
            try:
                # referencing model in delete (any other) statement allows other chained methods to
                # reference it with model as well. Essentially, not only do you delete from that table,
                # but you also can use its columns to compare to, perform operations on, etc...
                statement = delete(model).filter(model.name == schema.name)
                await session.execute(statement)
                await session.commit()
                return schema
            except SQLAlchemyError as err:
                raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                    detail=jsonable_encoder(ErrorSchema(
                                        err_type=err,
                                        err_message=err.orig.args[1]
                                    )))

    async def update_item(self, schema: BaseModel) -> Any:
        """
        :param schema: Schema that will be updated.
        :return: Results returned by selecting from core database.
        """
        model = resolve_model(schema)
        session = self.get_session()
        async with session as session:
            try:
                statement = update(model).filter(model.name == schema.name).values(schema.model_dump())
                await session.execute(statement)
                await session.commit()
                return schema
            except SQLAlchemyError as err:
                raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                    detail=jsonable_encoder(ErrorSchema(
                                        err_type=err,
                                        err_message=err.orig.args[1]
                                    )))

    async def select_item(self, schema: BaseModel) -> Any:
        """
        :param schema: Schema that will be selected.
        :return: Results returned by selecting from core database.
        """
        model = resolve_model(schema)
        session = self.get_session()
        async with session as session:
            try:
                statement = select(model).filter(model.name == schema.name)
                result = await session.execute(statement)
                retrieved = list(result.scalars().fetchall())
                final = [orm_to_schema(i) for i in retrieved]
                await session.commit()
                return final
            except SQLAlchemyError as err:
                raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                    detail=jsonable_encoder(ErrorSchema(
                                        err_type=err,
                                        err_message=err.orig.args[1]
                                    )))

    async def select_query(self, query: str) -> list[dict]:
        """
        :param query: an SQL query.
        :return: Results returned by selecting from core database.
        """
        session = self.get_session()
        async with session as session:
            try:
                if query is None:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                        detail=ErrorSchema(
                                            err_type=ValueError,
                                            err_message="Invalid query."
                                        ))
                model = await session.execute(statement=text(query))
                retrieved = list(model.fetchall())
                # doesn't work with columns with the same name
                headers = list(model.keys())
                final = list()
                for row in retrieved:
                    # OrderDict maintains the order inside a dictionary which is important since
                    # listing session results with .fetchall() sometimes swaps order of columns.
                    temp = OrderedDict()
                    for index, item in enumerate(headers):
                        temp.update({item: row[index]})
                    final.append(temp)
                await session.commit()
                return final
            except SQLAlchemyError as err:
                raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                    detail=jsonable_encoder(ErrorSchema(
                                        err_type=err,
                                        err_message=err.orig.args[1]
                                    )))
            except Exception as err:
                raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                    detail=jsonable_encoder(ErrorSchema(
                                        err_type=err,
                                        err_message=err.args[0]
                                    )))


CORE_DATABASE = MySQLDatabase()
