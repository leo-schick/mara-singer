"""Helper functions for sync. of a stream to a destination database"""

from mara_db import dbs

from .catalog import ReplicationMethod


def compose_delete_query(db_alias: str, target_table: str, replication_method: ReplicationMethod, replication_key = None) -> str:
    """
    Composes a delete query to a target table before starting sync.

    Args:
        db_alias
        target_table
        replication_method
        replication_key
    """
    db = dbs.db(db_alias)
    if replication_method == ReplicationMethod.FULL_TABLE:
        if isinstance(db, dbs.BigQueryDB):
            return f'DELETE FROM {target_table} WHERE 1=1' # BigQuery needs a where clause!
        else:
            return f'DELETE FROM {target_table}'
    elif replication_method == ReplicationMethod.INCREMENTAL:
        raise NotImplementedError()
    elif replication_method == ReplicationMethod.LOG_BASED:
        return None # delete nothing
