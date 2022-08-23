from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper
from dbacademy_helper.workspace_helper import WorkspaceHelper
from typing import Callable, TypeVar
T=TypeVar("T")

class DatabasesHelper:
    def __init__(self, workspace: WorkspaceHelper, da: DBAcademyHelper):
        self.da = da
        self.client = da.client
        self.workspace = workspace

    def _drop_databases_for(self, username: str):
        db_name = self.da.to_database_name(username=username, course_code=self.da.course_code)
        if db_name in self.workspace.existing_databases:
            print(f"Dropping the database \"{db_name}\" for {username}")
            dbgems.get_spark_session().sql(f"DROP DATABASE {db_name} CASCADE;")
        else:
            print(f"Skipping database drop for {username}")

    def drop_databases(self):
        self.workspace.do_for_all_users(lambda username: self._drop_databases_for(username=username))

        # Clear the list of databases (and derived users) to force a refresh
        self.workspace._usernames = None
        self.workspace._existing_databases = None

    def create_databases(self, drop_existing: bool, post_create: Callable[[], None] = None):
        self.workspace.do_for_all_users(lambda username: self.create_database_for(username=username,
                                                                                  drop_existing=drop_existing,
                                                                                  post_create=post_create))
        # Clear the list of databases (and derived users) to force a refresh
        self.workspace._usernames = None
        self.workspace._existing_databases = None

    def create_database_for(self, username: str, drop_existing: bool, post_create: Callable[[str], None] = None):
        db_name = self.da.to_database_name(username=username, course_code=self.da.course_code)
        db_path = f"dbfs:/mnt/dbacademy-users/{username}/{self.da.course_name}/database.db"

        if db_name in self.da.workspace.existing_databases and drop_existing:
            print(f"Dropping the database \"{db_name}\" for {username}")
            dbgems.get_spark_session().sql(f"DROP DATABASE {db_name} CASCADE;")

        print(f"Creating database \"{db_name}\" for {username}")
        dbgems.get_spark_session().sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_path}';")

        if post_create:
            # Call the post-create init function if defined
            post_create(db_name)

        return f"Created database {db_name}"
