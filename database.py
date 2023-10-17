# coding=utf-8

from __future__ import unicode_literals, absolute_import

import datetime
from contextlib import contextmanager

import sqlalchemy.orm.exc
from sqlalchemy.engine import Engine, URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, inspect, event
from sqlalchemy.orm import sessionmaker, Query, scoped_session
from objprint import add_objprint
import time
import warnings

Base = declarative_base()
warnings.filterwarnings("ignore")


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault("query_start_time", []).append(time.time())


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    total = time.time() - conn.info["query_start_time"].pop(-1)
    if total >= 1:
        print(f"Query: {statement}, 用时:{total: .3f}s")


class Engine:
    """原生sql操作"""

    def __init__(self, conn: dict):
        """
        创建连接
        :param conn: 数据库类型+驱动名称://用户名:密码@IP地址:端口号/数据库名称
        """
        db_type = f"{conn['dialect']}+{conn['driver']}" if conn['dialect'] and conn['driver'] \
            else conn['dialect'] + conn['driver']
        url = URL.create(
            db_type,
            username=conn['username'],
            password=conn['password'],
            host=conn['host'],
            port=conn['port'],
            database=conn['database']
        )
        self.engine = create_engine(url, echo=True)  # lazy类型，第一次使用才创建
        self.Base = automap_base()
        self.Base.prepare(autoload_with=self.engine)
        self.tables = self.Base.classes

    def execute(self, sql, **kwargs):
        """
        执行增删改查, 自动提交事务
        :param sql: sql语句
        :param kwargs:
        :return: <class 'sqlalchemy.engine.result.ResultProxy'>
        """
        return self.engine.execute(sql, **kwargs)

    def fetch_all(self, sql):
        """
        返回select结果的所有行
        :param sql:
        :return: <class 'sqlalchemy.engine.result.RowProxy'>
        """
        return self.execute(sql).fetchall()

    def fetch_any(self, sql, size=None):
        """
        返回select结果的任意行
        :param sql:
        :param size: 行数
        :return: <class 'sqlalchemy.engine.result.RowProxy'>
        """
        return self.execute(sql).fetchmany(size=size)


@add_objprint
class BaseModel(Engine):
    """对象关系映射(Object Relational Mapping)"""

    def __init__(self, conn):
        super().__init__(conn)
        _Session = scoped_session(sessionmaker(bind=self.engine))
        self.session = _Session()
        self.ins = inspect(self.engine)

    def __del__(self):
        self.session.close()

    @contextmanager
    def auto_commit(self):
        """
        自动提交或回滚事务, 使用格式为 with auto_commit():
        """
        try:
            yield self.session
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def create_all(self):
        """对所有继承Base的类执行建表, 若同名表已存在则不创建"""
        Base.metadata.create_all(bind=self.engine, checkfirst=True)

    def drop_all(self):
        """对所有继承Base的类执行删除表"""
        Base.metadata.drop_all(bind=self.engine)

    def drop_table(self, Table):
        """删除单表"""
        Table.__table__.drop(bind=self.engine)

    def select(self, *Table):
        """
        生成select语句
        :param Table: 表名或字段名
        :return: query
        """
        return self.session.query(*Table)

    def get(self, Table, value):
        """
        根据主键查询数据
        :param Table: 表名
        :param value: 主键的值, 格式为 5 or (5, 10) or {"id": 5, "version_id": 10}
        :return: 对象实例 or None
        """
        return self.session.query(Table).get(value)

    @staticmethod
    def get_all(query):
        """
        根据条件查询所有数据
        :param query: select语句
        :return: 实例列表 or []
        """
        return query.all()

    @staticmethod
    def get_first(query):
        """
        根据条件查询第一条数据
        :param query: select语句
        :return: 一个实例 or None
        """
        return query.first()

    @staticmethod
    def get_one(query):
        """
        根据条件查询一条数据, 要求仅有一条数据
        :param query: select语句
        :return: 一个实例
        """
        try:
            return query.one()
        except sqlalchemy.orm.exc.MultipleResultsFound:
            print(f"db_error: 查到多条数据\n{query}")
        except sqlalchemy.orm.exc.NoResultFound:
            print(f"db_error: 查不到数据\n{query}")

    def add(self, Table, data):
        """
        插入数据
        :param Table: 表名
        :param data: 数据, 格式为 dict
        """
        self.session.add(Table(**data))

    def get_schemas(self):
        """获取数据库名"""
        return self.ins.get_schema_names()

    def get_tables(self, schema):
        """
        获取表名
        :param schema: 数据库名
        :return: table list
        """
        return self.ins.get_table_names(schema=schema)

    def get_columns(self, Table, schema):
        """
        获取列信息
        :param Table: 表名
        :param schema: 数据库名
        :return: column list
        """
        return self.ins.get_columns(Table.__tablename__, schema=schema)

    @staticmethod
    def get_real_query(query):
        """
        获取实际执行的sql语句
        :param query: 默认生成的sql语句
        :return: 传入参数后的sql语句
        """
        return query.statement.compile(compile_kwargs={"literal_binds": True})

    @classmethod
    def to_dict(cls, obj):
        """将数据库查询出的对象转化为字典"""
        if isinstance(obj, list):
            # 查询结果为list, 说明使用的是all()
            result = []
            for o in obj:
                result.append(cls.to_dict(o))
        elif isinstance(obj, Query):
            result = None
            print("请先执行该sql语句!")
        else:
            # 不是list, 说明使用的是get()、first()、one()
            result = {}
            if isinstance(obj, sqlalchemy.engine.row.Row):
                # 如果是行对象, 说明查询的是表中部分字段
                if obj[-1]:  # 如果行对象最后一个元素非空, 说明使用了join
                    for i in obj:
                        result.update(cls.to_dict(i))
                else:
                    result = obj._mapping
            else:
                # 查询的是整个对象
                for key in obj.__mapper__.c.keys():
                    if isinstance(getattr(obj, key), datetime.datetime):
                        result[key] = str(getattr(obj, key))
                    else:
                        result[key] = getattr(obj, key)
        return result
