class TestCase(object):
    from typing import Callable, Any, List

    __slots__ = ('description', 'test_function', 'test_case_id', 'unique_id', 'depends_on', 'escape_html', 'points', 'hint')

    _LAST_ID = 0

    def __init__(self,
                 *,
                 suite,
                 test_function: Callable[[], Any],
                 description: str = None,
                 test_case_id: str = None,
                 depends_on: List[str] = None,
                 escape_html: bool = False,
                 points: int = 1,
                 hint=None):

        from typing import List
        import uuid

        # Because I cannot figure out how to resolve circular references
        expected_type = "<class 'dbacademy_helper.reality_checks.test_suite_class.TestSuite'>"
        assert str(type(suite)) == expected_type, f"Expected the parameter \"suite\" to be of type TestSuite, found {type(suite)}"

        if test_case_id is None:
            TestCase._LAST_ID += 1
            test_case_id = str(TestCase._LAST_ID)

        self.test_case_id = f"{suite.name}-{test_case_id}"

        self.hint = hint
        self.points = points
        self.escape_html = escape_html
        self.description = description
        self.test_function = test_function

        print(f"-"*80)
        print(f"Depends On:           {depends_on} | {type(depends_on)}")
        print(f"suite.last_test_id(): {suite.last_test_id()} | {type(suite.last_test_id())}")
        depends_on = depends_on or [suite.last_test_id()]

        self.depends_on = depends_on if type(depends_on) is List else [depends_on]
