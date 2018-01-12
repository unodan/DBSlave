########################################################################################################################
#    File: interface.py
# Purpose:
#  Author: Dan Huckson, https://github.com/unodan
########################################################################################################################
from abc import ABC, abstractmethod


class Instance(ABC):
    @abstractmethod
    def use(self, database_name):
        raise NotImplementedError('subclasses must override use()!')
    @abstractmethod
    def dump(self, database_name):
        raise NotImplementedError('subclasses must override dump()!')
    @abstractmethod
    def close(self):
        raise NotImplementedError('subclasses must override close()!')
    @abstractmethod
    def commit(self):
        raise NotImplementedError('subclasses must override commit()!')
    @abstractmethod
    def connect(self, database_name=None):
        raise NotImplementedError('subclasses must override connect()!')
    @abstractmethod
    def execute(self, sql, args=None):
        raise NotImplementedError('subclasses must override execute()!')
    @abstractmethod
    def fetchone(self):
        raise NotImplementedError('subclasses must override fetchone()!')
    @abstractmethod
    def fetchall(self):
        raise NotImplementedError('subclasses must override fetchall()!')
    @abstractmethod
    def drop_table(self, table_name):
        raise NotImplementedError('subclasses must override drop_table()!')
    @abstractmethod
    def drop_database(self, database_name):
        raise NotImplementedError('subclasses must override drop_database()!')
    @abstractmethod
    def create_table(self, table_name, sql):
        raise NotImplementedError('subclasses must override create_table()!')
    @abstractmethod
    def create_database(self, database_name):
        raise NotImplementedError('subclasses must override create_database()!')
    @abstractmethod
    def row_exist(self, table_name, id):
        raise NotImplementedError('subclasses must override row_exist()!')
    @abstractmethod
    def table_exist(self, table_name):
        raise NotImplementedError('subclasses must override table_exist()!')
    @abstractmethod
    def database_exist(self, database_name):
        raise NotImplementedError('subclasses must override database_exist()!')
    @abstractmethod
    def insert_row(self, table_name, row):
        raise NotImplementedError('subclasses must override insert_row()!')
    @abstractmethod
    def update_row(self, table_name, row, id):
        raise NotImplementedError('subclasses must override update_row()!')
    @abstractmethod
    def delete_row(self, table_name, id):
        raise NotImplementedError('subclasses must override delete_row()!')


class Credentials():
    def __init__(self):
        self.credentials = {}

    def set_credential(self, name, value):
        self.credentials[name] = value

    def set_credentials(self, crdntls):
        self.__credentials(crdntls)
        return self

    def get_credential(self, name):
        return self.credentials[name]

    def get_credentials(self):
        return self.credentials

    def __credentials(self, crdntls):
        fields = crdntls.split(':', 2)
        self.credentials["Driver"] = fields[0]

        if (fields[1][2:])[:1] == '/':
            try:
                parts = fields[1][2:].split('/')
                self.credentials["database"] = parts[1]
                self.credentials["charset"] = parts[2]
            except ValueError:
                print("ERROR: Improper connection string supplied.")
                exit(1)
        else:
            try:
                self.credentials["user"] = fields[1][2:]
                self.credentials["password"] = fields[2].split('@')[0]

                parts = fields[2].split('@')[1].split(':')
                if len(parts) == 2:
                    self.credentials["host"] = parts[0]
                    self.credentials["port"] = int(parts[1].split('/')[0])
                    self.credentials["database"] = parts[1].split('/')[1]
                    self.credentials["charset"] = parts[1].split('/')[2]
                else:
                    parts = fields[2].split('@')[1].split('/')
                    self.credentials["host"] = parts[0]
                    self.credentials["database"] = parts[1]
                    self.credentials["charset"] = parts[2]
            except:
                print("ERROR: Improper connection string supplied.")
                exit(1)


def new_engine(crdntls):
    crdntls = Credentials().set_credentials(crdntls).get_credentials()

    if crdntls["Driver"] == 'mysql':
        from dbslave import Maria
        return Maria.Interface(crdntls)

    elif crdntls["Driver"] == 'sqlite':
        from dbslave import SQLite3
        return SQLite3.Interface(crdntls)

    elif crdntls["Driver"] == 'postgres':
        from dbslave import Postgres
        return Postgres.Interface(crdntls)