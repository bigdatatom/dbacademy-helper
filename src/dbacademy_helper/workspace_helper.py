from dbacademy_gems import dbgems
from dbacademy_helper import DBAcademyHelper
from typing import Callable, List, TypeVar
T=TypeVar("T")

ALL_USERS = "All Users"
MISSING_USERS_ONLY = "Missing Users Only"
CURRENT_USER_ONLY = "Current User Only"

class WorkspaceHelper:

    def __init__(self, da: DBAcademyHelper):
        from dbacademy_helper.warehouses_helper import WarehousesHelper
        from dbacademy_helper.databases_helper import DatabasesHelper

        self.da = da
        self.client = da.client
        self.warehouses = WarehousesHelper(self, da)
        self.databases = DatabasesHelper(self, da)

        self._usernames = None
        self._existing_databases = None

        self.configure_for_options = ["", ALL_USERS, MISSING_USERS_ONLY, CURRENT_USER_ONLY]
        self.valid_configure_for_options = self.configure_for_options[1:]  # all but empty-string

    @property
    def configure_for(self):

        if self.da.is_smoke_test():
            # Under test, we are always configured for the current user only
            configure_for = CURRENT_USER_ONLY
        else:
            # Default to missing users only when called by a job (e.g. workspace automation)
            default_value = MISSING_USERS_ONLY if dbgems.is_job() else None
            configure_for = dbgems.get_parameter("configure_for", default_value)

        assert configure_for in self.valid_configure_for_options, f"Who the workspace is being configured for must be specified, found \"{configure_for}\". Options include {self.valid_configure_for_options}"
        return configure_for

    @property
    def usernames(self):
        if self._usernames is None:
            users = self.client.scim().users().list()
            self._usernames = [r.get("userName") for r in users]
            self._usernames.sort()

        if self.configure_for == CURRENT_USER_ONLY:
            # Override for the current user only
            return [self.da.username]

        elif self.configure_for == MISSING_USERS_ONLY:
            # The presumption here is that if the user doesn't have their own
            # database, then they are also missing the rest of their config.
            missing_users = []
            for user in self.usernames:
                db_name = self.da.get_database_name()
                if db_name not in self.existing_databases:
                    missing_users.append(user)

            missing_users.sort()
            return missing_users

        return self._usernames

    @property
    def existing_databases(self):
        if self._existing_databases is None:
            existing = dbgems.get_spark_session().sql("SHOW DATABASES").collect()
            self._existing_databases = {d[0] for d in existing}
        return self._existing_databases

    def do_for_all_users(self, f: Callable[[str], T]) -> List[T]:
        from multiprocessing.pool import ThreadPool

        # if self.usernames is None:
        #     raise ValueError("DBAcademyHelper.workspace.usernames must be defined before calling DBAcademyHelper.workspace.do_for_all_users(). See also DBAcademyHelper.workspace.load_all_usernames()")

        with ThreadPool(len(self.usernames)) as pool:
            return pool.map(f, self.usernames)

    @property
    def event_name(self):
        import re

        event_name = "Smoke Test" if self.da.is_smoke_test() else dbgems.get_parameter("event_name")
        assert event_name is not None and len(event_name) >= 3, f"The parameter event_name must be specified with min-length of 3, found \"{event_name}\"."

        event_name = re.sub(r"[^a-zA-Z\d]", "_", event_name)
        while "__" in event_name: event_name = event_name.replace("__", "_")

        return event_name

    @property
    def student_count(self):
        students_count = dbgems.get_parameter("students_count", "0").strip()
        students_count = int(students_count) if students_count.isnumeric() else 0
        students_count = max(students_count, len(self.usernames))
        return students_count
