
import os
from pathlib import Path
import psycopg2
import datetime
import inspect
from typing import NamedTuple, get_origin, get_args, Union

db_url = os.getenv('DB_URL')

PY_TO_PSQL = {
    int: 'INTEGER',
    str: 'TEXT',
    datetime.date: 'DATE',
    datetime.datetime: 'DATETIME',
    float: 'FLOAT',
    bool: 'BOOLEAN'
}

class CopySpec(NamedTuple):
    delimiter: str = '|'
    header: bool = True
    quote: str = '"'
    null: str = ''

class Field(NamedTuple):
    name: str
    psql_type: str
    is_nullable: bool

class Schema(NamedTuple):
    name: str
    fields: list[Field]

    def to_create_table(self):
        cols = ',\n\t'.join(
            f"{f.name} {f.psql_type}{' NOT NULL' if not f.is_nullable else ''}"
            for f in self.fields
        )
        q = f"""
        DROP TABLE IF EXISTS {self.name};
        CREATE TABLE {self.name} (
            {cols}
        )
        """
        return q
    
    def to_copy(self, copy: CopySpec = CopySpec()):
        """
        Get a "copy from stdin" cmd for use with copy_expert.
        """

        return f"""
        COPY {self.name} FROM STDIN WITH (
            FORMAT CSV,
            {'HEADER,' if copy.header else ''}
            QUOTE '{copy.quote}',
            NULL '{copy.null}'
        )
        """
    
    @classmethod
    def from_python(cls, py_schema: list[tuple[str, type]], table_name: str):
        fields = []
        is_nullable = False
        for name, t in py_schema:
            if get_origin(t) == Union and \
            len(args := get_args(t)) == 2 and \
            type(None) in args:
                t = next((a for a in args if a != type(None)))
                is_nullable = True
            if t not in PY_TO_PSQL:
                raise NotImplementedError(f'type {t} is not supported')
            
            fields.append(Field(name, PY_TO_PSQL[t], is_nullable))
        return Schema(table_name, fields)

    @classmethod
    def from_cls(cls, other_class, table_name: str | None = None) -> 'Schema':
        annotations = inspect.get_annotations(other_class)
        return cls.from_python([(n, t) for n, t in annotations.items()], table_name or other_class.__name__)


def run_query(q):
    with psycopg2.connect(db_url) as con, \
            con.cursor() as cur:
        cur.execute(q)

def load_csv(schema: Schema, path: Path, copy = CopySpec()):
    create = schema.to_create_table()
    copy = schema.to_copy()
    with psycopg2.connect(db_url) as con, \
            con.cursor() as cur, \
            open(path, 'r') as f:
        cur.execute('BEGIN')
        cur.execute(create)
        print(copy)
        cur.copy_expert(copy, f)
        cur.execute('COMMIT')
