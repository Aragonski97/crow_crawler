import threading
import mysql.connector
import pandas
import pandas as pd
import boto3
import os
from sqlalchemy import types, create_engine
from typing import Literal
from datetime import datetime
from io import StringIO
from abc import ABC, ABCMeta, abstractmethod


class ConnectionObject(ABC):
    __metaclass__ = ABCMeta

    def __init__(self, connection_config: dict, table_name: str | None = None):
        self.connection_config = connection_config
        self.table_name = table_name

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def upload_file(self, data: list, table_name: str | None = None):
        pass

    @abstractmethod
    def load_file(self, table_name: str | None = None, pd_filter: str = None):
        pass

    @abstractmethod
    def set_inactive_status(self, index: str, data: list, table_name: str | None = None):
        pass


class SQLConnection(ConnectionObject):

    def __init__(self, connection_config: dict, table_name: str | None = None) -> None:
        super().__init__(connection_config=connection_config, table_name=table_name)
        self.mysqlc = mysql.connector.connect(**self.connection_config)
        self.engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".format(*self.connection_config.values()),
                                    pool_pre_ping=True)

    def connect(self):
        assert self.mysqlc.is_connected()

    def upload_file(self, data: list, table_name: str | None = None):
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        df = pd.DataFrame(data).set_index('foreign_id')
        df.to_sql(name=table_name,
                  con=self.engine,
                  if_exists="append",
                  dtype={'foreign_id': types.BIGINT},
                  chunksize=10000)
        print(f"Output to loop.{table_name + ''} table finished.")

    def set_inactive_status(self, index: str, data: list, table_name: str | None = None) -> None:
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        cursor = self.mysqlc.cursor()
        items = [("inactive", item) for item in data]
        cursor.executemany(f"UPDATE {table_name} "
                           f"SET status = %s, "
                           f"date_found_inactive = '{datetime.now()}', "
                           f"days_active = DATEDIFF(date_found_inactive, date_extracted) "
                           f"WHERE {index} = %s", items)

    def load_file(self, pd_filter: str | None = None, table_name: str | None = None):
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        cursor = self.mysqlc.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {table_name}")
        if pd_filter is not None:
            df = pandas.DataFrame([item for item in cursor])
            return df[eval(pd_filter)]
        else:
            return pandas.DataFrame([item for item in cursor])

    def commit_sql_code(self, query: str, dictionary: bool = False) -> None:
        cursor = self.mysqlc.cursor(dictionary=dictionary)
        cursor.execute(query)

    def get_by_condition(self,
                         table_name: str,
                         columns: list,
                         condition: dict | None = None,
                         dictionary: bool = False) -> list:
        cursor = self.mysqlc.cursor(dictionary=dictionary)
        if condition is not None:
            cursor.execute(f"SELECT {','.join(columns)} "
                           f"FROM {table_name} as db "
                           f"WHERE {condition['column']} = '{condition['value']}'")
        else:
            cursor.execute(f"SELECT {','.join(columns)} FROM {table_name} as db")
        return [item for item in cursor]

    def get_custom_sql(self, sql_command: str, dictionary: bool = False) -> list:
        cursor = self.mysqlc.cursor(dictionary=dictionary)
        cursor.execute(sql_command)
        return [item for item in cursor]

    def sql_make_active(self, urls, table_name: str) -> None:
        cursor = self.mysqlc.cursor()
        cursor.executemany(f"UPDATE {table_name} as db "
                           f"SET db.status = %s, "
                           f"date_found_inactive = NULL, "
                           f"days_active = NULL "
                           f"WHERE db.url = %s", urls)

    def add_column(self, table_name: str, column_name: str, column_type: str, position: Literal["FIRST, AFTER"],
                   after_column: str | None = None) -> None:
        cursor = self.mysqlc.cursor()
        if position == "AFTER":
            assert after_column is not None
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type} {position} {after_column};")
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type} {position};")

    def add_key_column(self, table_name: str, column_name: str, column_type: type, index_column_name: str) -> None:
        cursor = self.mysqlc.cursor()
        if column_type is str:
            cursor.execute(f"CREATE INDEX {index_column_name} ON {table_name} ({column_type}(255));")
        if column_type is datetime:
            cursor.execute(f"CREATE INDEX {index_column_name} ON {table_name} ({column_name});")

    def drop_key_column(self, table_name: str, key_column: str) -> None:
        cursor = self.mysqlc.cursor()
        cursor.execute(f"ALTER TABLE {table_name} DROP INDEX {key_column};")

    def add_primary_column(self, table_name: str, primary_column: str) -> None:
        cursor = self.mysqlc.cursor()
        cursor.execute(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY(`{primary_column}`)")

    def cast_column_to_type(self, table_name: str, column_name: str, to_type: object):
        """ALTER TABLE table MODIFY column_name to_type; to_type should be an object of python type"""
        cursor = self.mysqlc.cursor()
        if isinstance(to_type, str):
            cursor.execute(f"ALTER TABLE {table_name} MODIFY {column_name} VARCHAR(255);")
        if isinstance(to_type, datetime):
            cursor.execute(f"ALTER TABLE {table_name} MODIFY {column_name} DATETIME;")
        if isinstance(to_type, int):
            cursor.execute(f"ALTER TABLE {table_name} MODIFY {column_name} INTEGER;")

    def delete_duplicates(self, table_name: str) -> None:
        cursor = self.mysqlc.cursor(dictionary=True)
        # the oldest duplicates are left!
        cursor.execute(f"DELETE p1 "
                       f"FROM {table_name} as p1 "
                       f"INNER JOIN (SELECT MIN(date_extracted) as date_extracted,"
                       f" foreign_id FROM {table_name} GROUP BY foreign_id) as p2 "
                       f"ON p1.foreign_id = p2.foreign_id "
                       f"WHERE p1.date_extracted != p2.date_extracted")


class AWSConnection(ConnectionObject):

    def __init__(self, connection_config: dict, table_name: dict | None = None) -> None:
        super().__init__(connection_config=connection_config, table_name=table_name)
        self.s3: boto3.session.Session.resource = None
        return

    def connect(self) -> bool:
        assert self.connection_config.get("service_name") is not None
        assert self.connection_config.get("region_name") is not None
        assert self.connection_config.get("aws_access_key_id") is not None
        assert self.connection_config.get("aws_secret_access_key") is not None
        assert self.connection_config.get("aws_upload_method") is not None
        assert self.connection_config.get("bucket") is not None
        self.s3 = boto3.resource(
            service_name=self.connection_config.get("service_name"),
            region_name=self.connection_config.get("region_name"),
            aws_access_key_id=self.connection_config.get("aws_access_key_id"),
            aws_secret_access_key=self.connection_config.get("aws_secret_access_key")
        )
        return True

    def upload_file(self, data: list, table_name: str | None = None):
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        csv_buffer = StringIO()
        pd.DataFrame(data).set_index("foreign_id").to_csv(path_or_buf=csv_buffer)
        self.s3.Object(self.connection_config.get("bucket"), table_name).put(Body=csv_buffer.getvalue())
        print(f"Output to {self.connection_config.get('bucket') + '/' + table_name} finished.")

    def load_file(self, pd_filter: str | None = None, table_name: str | None = None):
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        csv_obj = self.s3.get_object(Bucket=self.connection_config.get("bucket"))
        body = csv_obj[table_name]
        csv_string = body.read().decode('utf-8')
        if pd_filter is not None:
            return pd.DataFrame(pd.read_csv(StringIO(csv_string)))[eval(pd_filter)]
        else:
            return pd.DataFrame(pd.read_csv(StringIO(csv_string)))

    def set_inactive_status(self, index: str, data: list, table_name: str | None = None):
        pass


class CSVConnection(ConnectionObject):

    def __init__(self, connection_config: dict, table_name: str | None = None):
        super().__init__(connection_config=connection_config, table_name=table_name)

    def connect(self):
        assert self.connection_config.get("path") is not None
        assert self.connection_config.get("path").is_dir()

    def upload_file(self, data: list, table_name: str | None = None):
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        # pd.DataFrame(data).set_index("foreign_id").to_csv(path_or_buf=self.connection_config.get("path").joinpath(table_name))
        path = self.connection_config.get("path").joinpath(table_name)
        if os.path.isfile(path):
            pd.DataFrame(data).to_csv(path_or_buf=path, mode="a", header=False)
        else:
            pd.DataFrame(data).to_csv(path_or_buf=path, mode="a", header=True)
        print(f"Output to {self.connection_config.get('path')} finished by {threading.get_ident()}.")

    def load_file(self, table_name: str | None = None, pd_filter: str | None = None):
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        data = pandas.read_csv(self.connection_config.get("path").joinpath(table_name))

        # return [row.to_dict() for index, row in dataframe.iterrows()]
        # return [row.to_dict() for index, row in df.iterrows()]

        if pd_filter is not None:
            return pd.DataFrame(data)[eval(pd_filter)]
        else:
            return pd.DataFrame(data)

    def set_inactive_status(self, index: str, data: list, table_name: str | None = None):
        if table_name is None:
            assert self.table_name is not None
            table_name = self.table_name
        df = pd.read_csv(filepath_or_buffer=self.connection_config.get("path").joinpath(table_name))
        inactive_rows = df[df[index].isin(data)]
        if len(inactive_rows) == 0:
            return
        inactive_rows['status'] = 'inactive'
        inactive_rows['date_found_inactive'] = datetime.now()
        inactive_rows['days_active'] = (inactive_rows['date_found_inactive']
                                        - pd.to_datetime(inactive_rows['date_extracted'])).dt.days
        df.update(inactive_rows)
        df.to_csv(path_or_buf=self.connection_config.get("path").joinpath(table_name))
