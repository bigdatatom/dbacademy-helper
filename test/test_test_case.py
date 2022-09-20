import unittest


class MyTestCase(unittest.TestCase):

    def test_suite(self):
        from dbacademy_helper.reality_checks.test_suite_class import TestSuite
        suite = TestSuite("Whatever")

        pipeline_name = "Actual value"
        pipeline = "Some Value"
        suite.test_not_none(pipeline, description=f"The pipline named \"{pipeline_name}\" doesn't exist. Double check the spelling.")

        a = "apple"
        b = "banana"
        suite.test_equals(a, b)


if __name__ == '__main__':
    unittest.main()
