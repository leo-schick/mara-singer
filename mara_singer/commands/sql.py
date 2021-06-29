import typing as t

from mara_db import dbs
import mara_pipelines.config
from mara_page import _

from .singer import _SingerTapReadCommand

class SingerTapToDB(_SingerTapReadCommand):
    def __init__(self,
        tap_name: str, stream_selection: t.Union[t.List[str], t.Dict[str, t.List[str]]],
        target_schema: str,
        target_db_alias: str = None,
        config: dict = None,

        # optional args for manual config/catalog/state file handling; NOTE might be removed some day!
        config_file_name: str = None, catalog_file_name: str = None, state_file_name: str = None,

        # optional args for special calls; NOTE might be removed some day!
        use_state_file: bool = True,
        pass_state_file: bool = True) -> None:
        """
        Reads data from a singer.io tab and writes the content to a database schema.

        Args:
            tap_name: The tap command name (e.g. tap-exchangeratesapi)
            stream_selection: The selected streams, when the tap supports several streams. Can be given as stream array or as dict with the properties as value array.
            target_schema: The target database schema
            target_db_alias: The target database alias
            config: (default: None) A dict which is used to path the tap config file (when it exists) or create a temp config file (when it does not exists)
            config_file_name: (default: {tap_name}.json) The tap config file name
            catalog_file_name: (default: {tap_name}.json) The catalog file name
            state_file_name: (default: {tap_name}.json) The state file name
            use_state_file: (default: True) If the state file name should be passed to the tap command
            pass_state_file: (default: False) If the state file shall be passed to the tap. Is only passed when state_file_name is given.
        """
        super().__init__(tap_name,
            config=config, config_file_name=config_file_name,
            stream_selection=stream_selection,
            catalog_file_name=catalog_file_name if catalog_file_name else f'{tap_name}.json',
            state_file_name=state_file_name if state_file_name else (f'{tap_name}.json' if use_state_file else None),
            pass_state_file=pass_state_file)
        
        self._target_db_alias = target_db_alias
        self.target_schema = target_schema

    @property
    def target_db_alias(self):
        return self._target_db_alias or mara_pipelines.config.default_db_alias()

    def _target_name(self):
        db = dbs.db(self.target_db_alias)
        if isinstance(db, dbs.PostgreSQLDB):
            return 'target-postgres'
        elif isinstance(db, dbs.RedshiftDB):
            return 'target-redshift'
        elif isinstance(db, dbs.SQLiteDB):
            return 'target-sqlite'
        else:
            raise Exception(f'Not supported DB type {type(db)} for command SingerTapToDB')

    def _create_target_config(self, config: dict):
        db = dbs.db(self.target_db_alias)
        if isinstance(db, dbs.PostgreSQLDB):
            # Reference: https://github.com/datamill-co/target-postgres#configjson
            config.update({
                'postgres_host': db.host,
                'postgres_port': db.port,
                'postgres_database': db.database,
                'postgres_username': db.user,
                'postgres_password': db.password,
                'postgres_schema': self.target_schema,

                'postgres_sslmode': db.sslmode,
                'postgres_sslrootcert': db.sslrootcert,
                'postgres_sslcert': db.sslcert,
                'postgres_sslkey': db.sslkey
            })
        elif isinstance(db, dbs.RedshiftDB):
            # Reference: https://github.com/datamill-co/target-redshift#usage
            config.update({
                'redshift_host': db.host,
                'redshift_port': db.post,
                'redshift_database': db.database,
                'redshift_username': db.user,
                'redshift_password': db.password,
                'redshift_schema': self.target_schema,
                'target_s3': {
                    'aws_access_key_id': db.aws_access_key_id,
                    'aws_secret_access_key': db.aws_secret_access_key,
                    'aws_s3_bucket_name': db.aws_s3_bucket_name
                }
            })
        elif isinstance(db, dbs.SQLiteDB):
            # NOTE: self.target_schema is not used here because target-sqlite doesn't support this! ; we use optimistic behavior here and don't throw an error
            #       It would probably be better to implement a prefix option in target-sqlite instead of just ignoring self.target_schem

            # Reference: https://gitlab.com/meltano/target-sqlite
            config.update({
                'database': db.file_name
            })
        else:
            raise Exception(f'Not supported DB type {type(db)} for command SingerTapToDB')

    def html_doc_items(self) -> t.List[t.Tuple[str, str]]:
        doc = super().html_doc_items() + [
            ('target db', _.tt[self.target_db_alias]),
            ('target schema', self.target_schema)
        ]
        return doc
