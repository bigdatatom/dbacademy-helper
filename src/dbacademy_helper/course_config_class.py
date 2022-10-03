from typing import List

class CourseConfig:
    def __init__(self, *,
                 course_code: str,                # The abbreviated version of the course
                 course_name: str,                # The full name of the course, hyphenated
                 data_source_name: str,           # Should be the same as the course
                 data_source_version: str,        # New courses would start with 01
                 install_min_time: str,           # The minimum amount of time to install the datasets (e.g. from Oregon)
                 install_max_time: str,           # The maximum amount of time to install the datasets (e.g. from India)
                 remote_files: List[str],         # The enumerated list of files in the datasets
                 required_dbrs: List[str]):       # The enumerated list of DBRs supported by this course

        self.__course_code = course_code
        self.__course_name = course_name
        self.__data_source_name = data_source_name
        self.__data_source_version = data_source_version
        self.__install_min_time = install_min_time
        self.__install_max_time = install_max_time
        self.__remote_files = remote_files
        self.__required_dbrs = required_dbrs

    @property
    def course_code(self) -> str:
        return self.__course_code

    @property
    def course_name(self) -> str:
        return self.__course_name

    @property
    def data_source_name(self) -> str:
        return self.__data_source_name

    @property
    def data_source_version(self) -> str:
        return self.__data_source_version

    @property
    def install_min_time(self) -> str:
        return self.__install_min_time

    @property
    def install_max_time(self) -> str:
        return self.__install_max_time

    @property
    def remote_files(self) -> List[str]:
        return self.__remote_files

    @remote_files.setter
    def remote_files(self, remote_fies: List[str]):
        self.__remote_files = remote_fies

    @property
    def required_dbrs(self) -> List[str]:
        return self.__required_dbrs
