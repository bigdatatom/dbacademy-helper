from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper
from dbacademy_helper.workspace_helper import WorkspaceHelper
from typing import Callable, TypeVar
T=TypeVar("T")

class DatabasesHelper:
    def __init__(self, workspaces: WorkspaceHelper, da: DBAcademyHelper):
        self.da = da
        self.client = da.client
        self.workspaces = workspaces

    def create_databases(self, drop_existing: bool, post_create_init: Callable[[], None] = None):
        self.workspaces.do_for_all_users(lambda username: self.create_database_for(username=username,
                                                                                   drop_existing=drop_existing,
                                                                                   post_create_init=post_create_init))
        # Clear the list of databases (and derived users) to force a refresh
        self.workspaces._usernames = None
        self.workspaces._existing_databases = None

    def create_database_for(self, username: str, drop_existing: bool, post_create_init: Callable[[str], None]):
        db_name = self.da.to_database_name(username=username, course_code=self.da.course_code)
        db_path = f"dbfs:/mnt/dbacademy-users/{username}/{self.da.course_name}/database.db"

        if db_name in self.da.workspaces.databases and drop_existing:
            dbgems.get_spark_session().sql(f"DROP DATABASE {db_name} CASCADE;")

        # Create the database
        dbgems.get_spark_session().sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_path}';")

        if post_create_init:
            # Call the post-create init function if defined
            post_create_init(db_name)

        return f"Created database {db_name}"
