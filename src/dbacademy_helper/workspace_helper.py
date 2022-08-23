from dbacademy import dbgems
from dbacademy_helper import DBAcademyHelper, Paths
from typing import Callable, List
from typing import TypeVar
T=TypeVar("T")

ALL_USERS = "All Users"
MISSING_USERS_ONLY = "Missing Users Only"
CURRENT_USER_ONLY = "Current User Only"

class WorkspaceHelper:

    def __init__(self, da: DBAcademyHelper):
        self.da = da
        self.client = da.client

        self._usernames = None
        self._databases = None

        self.workspace_name = dbgems.get_browser_host_name()
        if not self.workspace_name: self.workspace_name = dbgems.get_notebooks_api_endpoint()

        self.org_id = dbgems.get_tag("orgId", "unknown")

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

        if self.configure_for == CURRENT_USER_ONLY:
            # Override for the current user only
            self._usernames = [self.da.username]

        elif self.configure_for == MISSING_USERS_ONLY:
            # The presumption here is that if the user doesn't have their own
            # database, then they are also missing the rest of their config.
            missing_users = []
            for user in self.usernames:
                db_name = self.da.get_database_name()
                if db_name not in self.databases:
                    missing_users.append(user)
            self._usernames = missing_users

        self._usernames.sort()
        return self._usernames

    @property
    def databases(self):
        if self._databases is None:
            databases = dbgems.get_spark_session().sql("SHOW DATABASES").collect()
            self._databases = {d[0] for d in databases}
        return self._databases

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

    @property
    def sql_warehouse_autoscale_min(self):
        return 1 if self.da.is_smoke_test() else 2  # math.ceil(self.students_count / 20)

    @property
    def sql_warehouse_autoscale_max(self):
        return 1 if self.da.is_smoke_test() else 20  # math.ceil(self.students_count / 5)

    # TODO - Change enable_serverless_compute to default to True once serverless is mainstream
    def create_shared_sql_warehouse(self, name: str = "Starter Warehouse", auto_stop_mins=120, enable_serverless_compute=False):
        from dbacademy.dbrest.sql.endpoints import RELIABILITY_OPTIMIZED, CHANNEL_NAME_CURRENT, CLUSTER_SIZE_2X_SMALL

        warehouse = self.client.sql.endpoints.create_or_update(
            name=name,
            cluster_size=CLUSTER_SIZE_2X_SMALL,
            enable_serverless_compute=enable_serverless_compute,
            min_num_clusters=self.sql_warehouse_autoscale_min,
            max_num_clusters=self.sql_warehouse_autoscale_max,
            auto_stop_mins=auto_stop_mins,
            enable_photon=True,
            spot_instance_policy=RELIABILITY_OPTIMIZED,
            channel=CHANNEL_NAME_CURRENT,
            tags={
                "dbacademy.event_name": self.da.clean_string(self.event_name),
                "dbacademy.students_count": self.da.clean_string(self.student_count),
                "dbacademy.workspace": self.da.clean_string(self.workspace_name),
                "dbacademy.org_id": self.da.clean_string(self.org_id),
            })
        warehouse_id = warehouse.get("id")

        # With the warehouse created, make sure that all users can attach to it.
        self.client.permissions.warehouses.update_group(warehouse_id, "users", "CAN_USE")

        print(f"Created warehouse \"{name}\" ({warehouse_id})")
        print(f"  Event Name:        {self.event_name}")
        print(f"  Configured for:    {self.configure_for}")
        print(f"  Student Count:     {self.student_count}")
        print(f"  Provisioning:      {len(self.usernames)}")
        print(f"  Autoscale minimum: {self.sql_warehouse_autoscale_min}")
        print(f"  Autoscale maximum: {self.sql_warehouse_autoscale_max}")
        if self.da.is_smoke_test:
            print(f"  Smoke Test:        {self.da.is_smoke_test()} ")
