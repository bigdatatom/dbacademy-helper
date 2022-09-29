class EnvConfig:
    def __init__(self, create_schema: bool, create_catalog: bool, requires_uc: bool):
        from dbacademy_gems import dbgems

        self.requires_uc = requires_uc

        self.__create_schema = create_schema   # Store the variable.
        self.__created_schema = create_schema  # Will be unconditionally equal

        if create_catalog: assert requires_uc, f"Inconsistent configuration: The parameter \"create_catalog\" was True and \"requires_uc\" was False."
        self.__create_catalog = create_catalog
        self.__created_catalog = False

        # self.create_uc_schema = False
        # self.create_uc_catalog = False
        # self.create_stock_schema = False
        # self.create_stock_catalog = False
        #
        # self.__created_uc_schema = False
        # self.__created_uc_catalog = False
        # self.__created_stock_schema = False
        # self.__created_stock_catalog = False
        #
        # self.schemas = []

        try: self.__username = dbgems.sql("SELECT current_user()").first()[0]
        except: self.__username = "unknown@example.com"  # Because of unit tests

    @property
    def username(self):
        return self.__username

    @property
    def created_catalog(self):
        return self.__created_catalog

    @property
    def create_catalog(self):
        return self.__create_catalog

    @property
    def created_schema(self):
        return self.__created_schema

    @property
    def create_schema(self):
        return self.__create_schema

    # @property
    # def created_uc_schema(self) -> bool:
    #     return self.__created_uc_schema
    #
    # @property
    # def created_uc_catalog(self) -> bool:
    #     return self.__created_uc_catalog
    #
    # @property
    # def created_stock_schema(self) -> bool:
    #     return self.__created_stock_schema
    #
    # @property
    # def created_stock_catalog(self) -> bool:
    #     return self.__created_stock_catalog

    @staticmethod
    def to_catalog_name(username):
        import re, hashlib
        from .dbacademy_helper_class import DBAcademyHelper

        local_part = username.split("@")[0]  # Split the username, dropping the domain
        value = hashlib.sha3_512(username.encode('utf-8')).hexdigest()
        username_hash = abs(int(re.sub(r"[a-z]", "", value))) & 10000
        return DBAcademyHelper.clean_string(f"{local_part}-{username_hash}-dbacademy").lower()

    @staticmethod
    def to_schema_name(username, course_code) -> str:
        """
        Given the specified username and course_code, creates a database name that follows the pattern "da-name_prefix@hash-course_code"
        where name_prefix is the right hand of an email as in "john.doe" given "john.doe@example.com", hash is truncated hash based on
        the full email address and course code.
        :param username: The full username (e.g. email address) to compose the database name from.
        :param course_code: The abbreviated version of the course's name
        :return: Returns the name of the database for the given user and course.
        """
        import re
        from .dbacademy_helper_class import DBAcademyHelper

        schema_name, da_hash = DBAcademyHelper.to_username_hash(username, course_code)
        schema_name = f"da-{schema_name}@{da_hash}-{course_code}"                # Composite all the values to create the "dirty" database name
        schema_name = re.sub(r"[^a-zA-Z\d]", "_", schema_name)                   # Replace all special characters with underscores (not digit or alpha)
        while "__" in schema_name: schema_name = schema_name.replace("__", "_")  # Replace all double underscores with single underscores
        return schema_name
