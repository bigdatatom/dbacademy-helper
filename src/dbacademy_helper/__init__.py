try: import dbacademy.dbgems
except ImportError: raise Exception("The runtime dependency dbgems was not found. Please install https://github.com/databricks-academy/dbacademy-gems")

try: import dbacademy.dbrest
except ImportError: raise Exception("The runtime dependency dbrest was not found. Please install https://github.com/databricks-academy/dbacademy-rest")

import pyspark
from typing import Union
from dbacademy import dbgems

from typing import Callable, List, TypeVar
T=TypeVar("ReturnType")

class Paths:
    def __init__(self, working_dir_root: str, working_dir: str, datasets: str, user_db: Union[str, None], enable_streaming_support: bool):

        self.user_db = user_db
        self.datasets = datasets
        self.working_dir = working_dir

        self._working_dir_root = working_dir_root

        # When working with streams, it helps to put all checkpoints in their
        # own directory relative the previously defined working_dir
        if enable_streaming_support:
            self.checkpoints = f"{working_dir}/_checkpoints"

    # noinspection PyGlobalUndefined
    @staticmethod
    def exists(path):
        global dbutils

        """
        Returns true if the specified path exists else false.
        """
        try:
            return len(dbgems.get_dbutils().fs.ls(path)) >= 0
        except Exception:
            return False

    def print(self, padding="  ", self_name="self."):
        """
        Prints all the paths attached to this instance of Paths
        """
        max_key_len = 0
        for key in self.__dict__:
            if not key.startswith("_"):
                max_key_len = len(key) if len(key) > max_key_len else max_key_len

        for key in self.__dict__:
            if not key.startswith("_"):
                label = f"{padding}{self_name}paths.{key}: "
                print(label.ljust(max_key_len + 13) + self.__dict__[key])

    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n").replace("{", "").replace("}", "").replace("'", "")


class DBAcademyHelper:
    def __init__(self,
                 course_code: str,
                 course_name: str,
                 data_source_name: str,
                 data_source_version: str,
                 install_min_time: str,
                 install_max_time: str,
                 enable_streaming_support: bool,
                 remote_files: list,
                 lesson: str = None,
                 asynchronous: bool = True):

        import re, time
        from dbacademy.dbrest import DBAcademyRestClient

        self.start = int(time.time())
        self.spark = dbgems.get_spark_session()

        self.create_db = None
        self.course_code = course_code
        self.course_name = course_name
        self.remote_files = remote_files
        self.naming_params = {"course": course_code}
        self.install_min_time = install_min_time
        self.install_max_time = install_max_time
        self.data_source_name = data_source_name
        self.data_source_version = data_source_version
        self.enable_streaming_support = enable_streaming_support

        self.client = DBAcademyRestClient()
        self.usernames = None

        # Are we running under test? If so we can "optimize" for parallel execution
        # without affecting the student's runtime-experience. As in the student can
        # use one working directory and one database, but under test, we can use many
        if lesson is None and asynchronous and self.is_smoke_test():
            # The developer did not define a lesson, we can run asynchronous, and this
            # is a smoke test, so we can define a lesson here for the sake of testing
            lesson = str(abs(hash(dbgems.get_notebook_path())) % 10000)

        self.lesson = None if lesson is None else lesson.lower()

        # Define username using the hive function (cleaner than notebooks API)
        self.username = self.spark.sql("SELECT current_user()").first()[0]

        # Create the database name prefix according to curriculum standards. This
        # is the value by which all databases in this course should start with.
        # Besides, creating this lesson's database name

        self.db_name_prefix = self.to_database_name(username=self.username, course_code=self.course_code)

        # This is the location in our Azure data repository of the datasets for this lesson
        self.data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{self.data_source_name}/{self.data_source_version}"

        # This is the common super-directory for each lesson, removal of which
        # is designed to ensure that all assets created by students is removed.
        # As such, it is not attached to the path object to hide it from
        # students. Used almost exclusively in the Rest notebook.
        working_dir_root = f"dbfs:/mnt/dbacademy-users/{self.username}/{self.course_name}"

        # This is where the datasets will be downloaded to and should be treated as read-only for all pratical purposes
        datasets_path = f"dbfs:/mnt/dbacademy-datasets/{self.data_source_name}/{self.data_source_version}"

        if self.lesson is None:
            self.clean_lesson = None
            working_dir = working_dir_root  # No lesson, working dir is same as root
            self.paths = Paths(working_dir_root=working_dir_root,
                               working_dir=working_dir,
                               datasets=datasets_path,
                               user_db=f"{working_dir}/database.db",
                               enable_streaming_support=enable_streaming_support)
            # self.hidden = Paths(working_dir, None, enable_streaming_support)  # Create the "hidden" path
            self.db_name = self.db_name_prefix  # No lesson, database name is the same as prefix
        else:
            working_dir = f"{working_dir_root}/{self.lesson}"  # Working directory now includes the lesson name
            self.clean_lesson = re.sub(r"[^a-zA-Z\d]", "_", self.lesson.lower())  # Replace all special characters with underscores
            self.paths = Paths(working_dir_root=working_dir_root,
                               working_dir=working_dir,
                               datasets=datasets_path,
                               user_db=f"{working_dir}/{self.clean_lesson}.db",
                               enable_streaming_support=enable_streaming_support)
            # self.hidden = Paths(working_dir, self.clean_lesson, enable_streaming_support)  # Create the "hidden" path
            self.db_name = f"{self.db_name_prefix}_{self.clean_lesson}"  # Database name includes the lesson name

    def get_database_name(self):
        """
        Alias for DBAcademyHelper.to_database_name(self.username, self.course_code)
        :return: Returns the name of the database for the current user and course.
        """
        return DBAcademyHelper.to_database_name(self.username, self.course_code)

    @staticmethod
    def to_database_name(username, course_code) -> str:
        """
        Given the specified username and course_code, creates a database name that follows the pattern "da-name_prefix@hash-course_code"
        where name_prefix is the right hand of an email as in "john.doe" given "john.doe@example.com", hash is truncated hash based on
        the full email address and course code.
        :param username: The full username (e.g. email address) to compose the database name from.
        :param course_code: The abbreviated version of the course's name
        :return: Returns the name of the database for the given user and course.
        """
        import re
        da_name, da_hash = DBAcademyHelper.to_username_hash(username, course_code)
        db_name = f"da-{da_name}@{da_hash}-{course_code}"            # Composite all the values to create the "dirty" database name
        db_name = re.sub(r"[^a-zA-Z\d]", "_", db_name)               # Replace all special characters with underscores (not digit or alpha)
        while "__" in db_name: db_name = db_name.replace("__", "_")  # Replace all double underscores with single underscores
        return db_name

    def get_username_hash(self):
        """
        Alias for DBAcademyHelper.to_username_hash(self.username, self.course_code)
        :return: Returns (da_name:str, da_hash:str)
        """
        return DBAcademyHelper.to_username_hash(self.username, self.course_code)

    @staticmethod
    def to_username_hash(username: str, course_code: str) -> (str, str):
        """
        Utility method to split the specified user's email address, dropping the domain, and then creating a hash based on the
        full email address and the specified course_code. The primary usage of this function is in creating the user's database,
        but is also used in creating SQL Endpoints, DLT Piplines, etc - any place we need a short, student-specific name.

        :param username: The full username (e.g. email address) to compose the hash from.
        :param course_code: The abbreviated version of the course's name
        :return: Returns (da_name:str, da_hash:str)
        """

        da_name = username.split("@")[0]  # Split the username, dropping the domain
        da_hash = abs(hash(f"{username}-{course_code}")) % 10000  # Create a has from the full username and course code
        return da_name, da_hash

    @staticmethod
    def monkey_patch(function_ref, delete=True):
        """
        This function "monkey patches" the specified function to the DBAcademyHelper class. While not 100% necissary, this pattern does allow each function to be defined in it's own cell which makes authoring notebooks a little bit easier.
        """
        import inspect

        signature = inspect.signature(function_ref)
        assert "self" in signature.parameters, f"""Missing the required parameter "self" in the function "{function_ref.__name__}()" """

        setattr(DBAcademyHelper, function_ref.__name__, function_ref)

        return None if delete else function_ref

    def init(self, install_datasets, create_db):
        """
        This function aims to set up the environment enabling the constructor to provide initialization of attributes only and thus not modifying the environment upon initialization.
        """

        self.create_db = create_db  # Flag to indicate if we are creating the database or not

        if install_datasets:
            self.install_datasets()

        if create_db:
            # print(f"\nCreating the database \"{self.db_name}\"")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.paths.user_db}'")
            self.spark.sql(f"USE {self.db_name}")

    def reset_environment(self):
        return self.cleanup(validate_datasets=False)

    def cleanup(self, validate_datasets=True):
        """
        Cleans up the user environment by stopping any active streams, dropping the database created by the call to init() and removing the user's lesson-specific working directory and any assets created in that directory.
        """

        # Clear any cached values from previous lessons
        self.spark.catalog.clearCache()

        active_streams = len(self.spark.streams.active) > 0
        remove_wd = self.paths.exists(self.paths.working_dir)
        drop_db = self.spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1

        if drop_db or remove_wd or active_streams:
            print("Resetting the learning environment...")

        for stream in self.spark.streams.active:
            print(f"...stopping the stream \"{stream.name}\"")
            stream.stop()
            try:
                stream.awaitTermination()
            except:
                pass  # Bury any exceptions

        if drop_db:
            print(f"...dropping the database \"{self.db_name}\"")
            self.spark.sql(f"DROP DATABASE {self.db_name} CASCADE")

        if remove_wd:
            print(f"...removing the working directory \"{self.paths.working_dir}\"")
            dbgems.get_dbutils().fs.rm(self.paths.working_dir, True)

        if validate_datasets:
            self.validate_datasets(fail_fast=False, repairing_dataset=False)

    def conclude_setup(self):
        """
        Concludes the setup of DBAcademyHelper by advertising to the student the new state of the environment such as predefined path variables, databases and tables created on behalf of the student and the total setup time. Additionally, all path attributes are pushed to the Spark context for reference in SQL statements.
        """

        import time

        # Inject the user's database name
        # Add custom attributes to the SQL context here.
        self.spark.conf.set("da.db_name", self.db_name)
        self.spark.conf.set("DA.db_name", self.db_name)

        # Automatically add all path attributes to the SQL context as well.
        for key in self.paths.__dict__:
            if not key.startswith("_"):
                value = self.paths.__dict__[key]
                self.spark.conf.set(f"da.paths.{key.lower()}", value)
                self.spark.conf.set(f"DA.paths.{key.lower()}", value)

        if self.create_db:
            print(f"\nPredefined tables in \"{self.db_name}\":")
            tables = self.spark.sql(f"SHOW TABLES IN {self.db_name}").filter("isTemporary == false").select("tableName").collect()
            if len(tables) == 0: print("  -none-")
            for row in tables: print(f"  {row[0]}")

        print("\nPredefined paths variables:")
        self.paths.print(self_name="DA.")

        print(f"\nSetup completed in {int(time.time()) - self.start} seconds")

    def install_datasets(self, reinstall_datasets=False, repairing_dataset=False):
        """
        Install the datasets used by this course to DBFS.

        This ensures that data and compute are in the same region which subsequently mitigates performance issues
        when the storage and compute are, for example, on opposite sides of the world.
        """
        import time

        # if not repairing_dataset: print(f"\nThe source for the datasets is\n{self.data_source_uri}/")
#        if not repairing_dataset: print(f"\nYour local dataset directory is {self.paths.datasets}")

        if Paths.exists(self.paths.datasets):
            # It's already installed...
            if reinstall_datasets:
                if not repairing_dataset: print(f"\nRemoving previously installed datasets")
                dbgems.get_dbutils().fs.rm(self.paths.datasets, True)

            if not reinstall_datasets:
                print(f"\nSkipping install of existing datasets to \"{self.paths.datasets}\"")
                self.validate_datasets(fail_fast=False, repairing_dataset=False)
                return

        print(f"\nInstalling datasets...")
        print(f"...from \"{self.data_source_uri}\"")
        print(f"...to \"{self.paths.datasets}\"")

        print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
              region that your workspace is in, this operation can take as little as {self.install_min_time} and 
              upwards to {self.install_max_time}, but this is a one-time operation.""")

        # Using data_source_uri is a temporary hack because it assumes we can actually
        # reach the remote repository - in cases where it's blocked, this will fail.
        files = dbgems.get_dbutils().fs.ls(self.data_source_uri)

        what = "dataset" if len(files) == 1 else "datasets"
        print(f"\nInstalling {len(files)} {what}: ")

        install_start = int(time.time())
        for f in files:
            start = int(time.time())
            print(f"Copying /{f.name[:-1]}", end="...")

            source_path = f"{self.data_source_uri}/{f.name}"
            target_path = f"{self.paths.datasets}/{f.name}"

            dbgems.get_dbutils().fs.cp(source_path, target_path, True)
            print(f"({int(time.time()) - start} seconds)")

        self.validate_datasets(fail_fast=True, repairing_dataset=repairing_dataset)

        print(f"""\nThe install of the datasets completed successfully in {int(time.time()) - install_start} seconds.""")

    def print_copyrights(self):
        datasets = [f.path for f in dbgems.get_dbutils().fs.ls(self.paths.datasets)]
        for dataset in datasets:
            readme_path = f"{dataset}README.md"
            try:
                head = dbgems.get_dbutils().fs.head(readme_path)
                lines = len(head.split("\n")) + 1
                html = f"""<html><body><h1>{dataset}</h1><textarea rows="{lines}" style="width:100%; overflow-x:scroll">{head}</textarea></body></html>"""
                self.display_html(html)
            except:
                html = f"""<html><body><h1>{dataset}</h1><textarea rows="3" style="width:100%; overflow-x:scroll">**ERROR**\nREADME.md was not found</textarea></body></html>"""
                self.display_html(html)

    def list_r(self, path, prefix=None, results=None):
        """
        Utility method used by the dataset validation, this method performs a recursive list of the specified path and returns the sorted list of paths.
        """
        if prefix is None: prefix = path
        if results is None: results = list()

        try:
            files = dbgems.get_dbutils().fs.ls(path)
        except:
            files = []

        for file in files:
            data = file.path[len(prefix):]
            results.append(data)
            if file.isDir():
                self.list_r(file.path, prefix, results)

        results.sort()
        return results

    def enumerate_remote_datasets(self):
        """
        Development function used to enumerate the remote datasets for use in validate_datasets()
        """
        files = self.list_r(self.data_source_uri)
        files = "remote_files = " + str(files).replace("'", "\"")

        self.display_html(f"""
            <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
            <textarea rows="10" style="width:100%">{files}</textarea>
        """)

    def enumerate_local_datasets(self):
        """
        Development function used to enumerate the local datasets for use in validate_datasets()
        """
        files = self.list_r(self.paths.datasets)
        files = "remote_files = " + str(files).replace("'", "\"")

        self.display_html(f"""
            <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
            <textarea rows="10" style="width:100%">{files}</textarea>
        """)

    def do_validate(self):
        """
        Utility method to compare local datasets to the registered list of remote files.
        """
        import time

        start = int(time.time())
        local_files = self.list_r(self.paths.datasets)

        errors = []

        for file in local_files:
            if file not in self.remote_files:
                what = "path" if file.endswith("/") else "file"
                errors.append(f"...Extra {what}: {file}")
                break

        for file in self.remote_files:
            if file not in local_files:
                what = "path" if file.endswith("/") else "file"
                errors.append(f"...Missing {what}: {file}")
                break

        print(f"({int(time.time()) - start} seconds)")
        for error in errors:
            print(error)

        return len(errors) == 0

    def validate_datasets(self, fail_fast: bool, repairing_dataset: bool):
        """
        Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
        """

        if repairing_dataset:
            print(f"\nRevalidating the locally installed datasets", end="...")
        else:
            print(f"\nValidating the locally installed datasets", end="...")

        result = self.do_validate()

        if not result:
            if fail_fast:
                raise Exception("Validation failed - see previous messages for more information.")
            else:
                print("...Attempting to repair locally installed dataset")
                self.install_datasets(reinstall_datasets=True, repairing_dataset=True)

    def run_high_availability_job(self, job_name, notebook_path):

        # job_name = f"DA-{self.course_name}-Configure-Permissions"
        self.client.jobs().delete_by_name(job_name, success_only=False)

        # notebook_path = f"{dbgems.get_notebook_dir()}/Configure-Permissions"

        params = {
            "name": job_name,
            "tags": {
                "dbacademy.course": self.course_name,
                "dbacademy.source": self.course_name
            },
            "email_notifications": {},
            "timeout_seconds": 7200,
            "max_concurrent_runs": 1,
            "format": "MULTI_TASK",
            "tasks": [
                {
                    "task_key": "Configure-Permissions",
                    "description": "Configure all user's permissions for user-specific databases.",
                    "libraries": [],
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": []
                    },
                    "new_cluster": {
                        "num_workers": "0",
                        "spark_conf": {
                            "spark.master": "local[*]",
                            "spark.databricks.acl.dfAclsEnabled": "true",
                            "spark.databricks.repl.allowedLanguages": "sql,python",
                            "spark.databricks.cluster.profile": "serverless"
                        },
                        "runtime_engine": "STANDARD",
                        "spark_env_vars": {
                            "WSFS_ENABLE_WRITE_SUPPORT": "true"
                        },
                    },
                },
            ],
        }
        cluster_params = params.get("tasks")[0].get("new_cluster")
        cluster_params["spark_version"] = self.client.clusters().get_current_spark_version()

        if self.client.clusters().get_current_instance_pool_id() is not None:
            cluster_params["instance_pool_id"] = self.client.clusters().get_current_instance_pool_id()
        else:
            cluster_params["node_type_id"] = self.client.clusters().get_current_node_type_id()

        create_response = self.client.jobs().create(params)
        job_id = create_response.get("job_id")

        run_response = self.client.jobs().run_now(job_id)
        run_id = run_response.get("run_id")

        final_response = self.client.runs().wait_for(run_id)

        final_state = final_response.get("state").get("result_state")
        assert final_state == "SUCCESS", f"Expected the final state to be SUCCESS, found {final_state}"

        self.client.jobs().delete_by_name(job_name, success_only=False)

        print()
        print("Update completed successfully.")

    @staticmethod
    def init_mlflow_as_job():
        """
        Used to initialize MLflow with the job ID when ran under test.
        """
        import mlflow

        if dbgems.get_job_id():
            mlflow.set_experiment(f"/Curriculum/Test Results/Experiments/{dbgems.get_job_id()}")

    @staticmethod
    def display_html(html) -> None:
        import inspect
        caller_frame = inspect.currentframe().f_back
        while caller_frame is not None:
            caller_globals = caller_frame.f_globals
            function = caller_globals.get("displayHTML")
            if function:
                return function(html)
            caller_frame = caller_frame.f_back
        raise ValueError("displayHTML not found in any caller frames.")

    @staticmethod
    def clean_string(value):
        import re
        value = re.sub(r"[^a-zA-Z\d]", "_", str(value))
        while "__" in value: value = value.replace("__", "_")
        return value

    @staticmethod
    def sql(sql, echo=True):
        if echo:
            # Thread safe printing.
            print(" -- " + sql + ";\n", end="")
        return dbgems.get_spark_session().sql(sql)

    def load_all_usernames(self):
        self.usernames = [r.get("userName") for r in self.client.scim().users().list()]
        self.usernames.sort()
        return self.usernames

    def do_for_all_users(self, f: Callable[[str], T]) -> List[T]:
        if self.usernames is None:
            raise ValueError("DBAcademyHelper.usernames must be defined before calling DBAcademyHelper.do_for_all_users(). See also DBAcademyHelper.load_all_usernames()")

        from multiprocessing.pool import ThreadPool
        with ThreadPool(len(self.usernames)) as pool:
            return pool.map(f, self.usernames)

    @property
    def is_smoke_test_key(self):
        return "dbacademy.smoke-test"

    def is_smoke_test(self):
        """
        Helper method to indentify when we are running as a smoke test
        :return: Returns true if the notebook is running as a smoke test.
        """
        return dbgems.get_spark_session().conf.get(self.is_smoke_test_key, "false").lower() == "true"

    @staticmethod
    def _get_or_find_query(query: Union[str, pyspark.sql.streaming.StreamingQuery], delay_seconds: int):
        import time

        if type(query) == pyspark.sql.streaming.StreamingQuery:
            return query

        queries = [q for q in dbgems.get_spark_session().streams.active if q.name == query]

        while len(queries) == 0:
            print("The query is not yet active...")

        time.sleep(delay_seconds)  # Give it a couple of seconds

        queries = [q for q in dbgems.get_spark_session().streams.active if q.name == query]

        if len(queries) == 1:
            return queries[0]
        elif len(queries) > 1:
            raise ValueError(f"More than one spark query was found named {query}")

    @staticmethod
    def block_until_stream_is_ready(query: Union[str, pyspark.sql.streaming.StreamingQuery], min_batches: int = 2, delay_seconds: int = 5):
        """
        A utility method used in streaming notebooks to block until the stream has processed n batches. This method serves one main purpose in two different use cases.

        The purpose is to block the current command until the state of the stream is ready and thus allowing the next command to execute against the properly initialized stream.

        The first use case is in jobs where the stream is started in one cell but execution of subsequent cells start prematurely.

        The second use case is to slow down students who likewise attempt to execute subsequent cells before the stream is in a valid state either by invoking subsequent cells directly or by execute the Run-All Command

        :param query: An instance of a query object or the name of the query
        :param min_batches: The minimum number of batches to be processed before allowing execution to continue.
        :param delay_seconds: The amount of delay in seconds between each test.
        :return:
        """
        import time

        if query is None:
            raise ValueError(f"Expected the parameter query to be of type str or pyspark.sql.streaming.StreamingQuery, found {type(query)}")
        else:
            q = DBAcademyHelper._get_or_find_query(query=query, delay_seconds=delay_seconds)

        while True:
            count = len(q.recentProgress)
            print(f"Processed {count + 1} of {min_batches} batches...")

            if not q.isActive:
                print("The query is no longer active...")
                break
            elif count >= min_batches:
                break

            time.sleep(delay_seconds)  # Give it a couple of seconds

        count = len(q.recentProgress)
        print(f"The stream is now active with {count} batches having been processed.")
