"""SQLAlchemy models of the run index.

The same models serve every backend (SQLite by default, MySQL/MariaDB when configured).
Rows are summaries of runs/<run_id>/state.json, which stays authoritative: the tables are
derived data and can be rebuilt from the JSON files at any time.

One database can hold the index of several WORK_DIRs: every row carries a ``namespace``
(see config.DatabaseConfig), and queries, rebuilds and initialization act on one namespace.
Columns are plain strings (timestamps are the ISO strings of state.json), so values read
back are exactly what was written.
"""

from sqlalchemy import Column, Index, String, Text
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import DeclarativeBase, object_mapper

# MySQL/MariaDB (utf8mb4, 4 bytes per character) index key parts must fit 767 bytes on old
# servers: 191 characters.
KEY_LEN = 191


def key_string(length=KEY_LEN):
    """A key column: case-sensitive on MySQL/MariaDB too (their default collation is not, so
    run IDs 'Run1' and 'run1', or two paths differing in case, would collide)."""
    exact = mysql.VARCHAR(length, charset="utf8mb4", collation="utf8mb4_bin")
    return String(length).with_variant(exact, "mysql", "mariadb")


class Base(DeclarativeBase):
    """Declarative base of the globusiri tables."""


class ModelBase:
    """dict-like access to a model's columns."""

    def update(self, values):
        for k, v in values.items():
            self[k] = v

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        return iter(self.items())

    def keys(self):
        return [k for k, _ in self.items()]

    def values(self):
        return [v for _, v in self.items()]

    def items(self):
        return [(attr.key, getattr(self, attr.key)) for attr in object_mapper(self).column_attrs]

    def to_dict(self):
        return {k: v for k, v in self.items() if not k.startswith("_")}


class Run(Base, ModelBase):
    """One summary row per run (see runindex.summary)."""

    __tablename__ = "globusiri_runs"

    namespace = Column(key_string(), primary_key=True)
    run_id = Column(key_string(), primary_key=True)
    site = Column(String(255))
    status = Column(String(32))          # active | finished | failed | cancelled | NULL (made by `job submit`)
    current_step = Column(String(64))
    step_status = Column(String(32))
    step_state = Column(String(64))      # IRI job state while monitoring
    job_id = Column(String(255))
    created_at = Column(String(64))
    updated_at = Column(String(64))
    finished_at = Column(String(64))
    message = Column(Text)               # last_error while active, final message otherwise

    __table_args__ = (
        Index("globusiri_runs_status", "namespace", "status", "created_at"),
        Index("globusiri_runs_site", "namespace", "site", "created_at"),
    )


class Meta(Base, ModelBase):
    """Bookkeeping of the index itself, per namespace (e.g. 'initialized')."""

    __tablename__ = "globusiri_meta"

    namespace = Column(key_string(), primary_key=True)
    key = Column(key_string(64), primary_key=True)
    value = Column(String(255))


# Summary columns, in the order rows are returned (namespace is internal).
COLUMNS = tuple(c.name for c in Run.__table__.columns if c.name != "namespace")


def register_models(engine):
    """Create the missing tables and indexes."""
    Base.metadata.create_all(engine)


def unregister_models(engine):
    """Drop the tables (tests and tooling only)."""
    Base.metadata.drop_all(engine)
