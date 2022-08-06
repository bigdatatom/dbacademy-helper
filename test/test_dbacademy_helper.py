import unittest
from dbacademy.dbhelper import DBAcademyHelper


class MyTestCase(unittest.TestCase):
    def __init__(self):
        super().__init__()
        self.helper = DBAcademyHelper(
            course_code="ut",
            course_name="Unit Test",
            data_source_name="unit-test",
            data_source_version="unit-test",
            install_min_time="3 min",
            install_max_time="9 min",
            enable_streaming_support=False,
            remote_files=["/README.md"],
            lesson="Random Lesson",
            asynchronous=True)

    def test_to_database_name(self):
        username = "john.doe@example.com"
        course_code = self.helper.course_code
        db_name = DBAcademyHelper.to_database_name(username, course_code)

        self.assertEqual("adf", db_name)


if __name__ == '__main__':
    unittest.main()
