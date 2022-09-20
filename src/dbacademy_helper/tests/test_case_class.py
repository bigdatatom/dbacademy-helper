class TestCase(object):
    from typing import Callable, Any, List

    __slots__ = ('description', 'test_function', 'test_case_id', 'unique_id', 'depends_on', 'escape_html', 'points', 'hint', "actual_value")

    _LAST_ID = 0

    def __init__(self,
                 *,
                 suite,
                 test_function: Callable[[], Any],
                 description: str,
                 actual_value: Any,
                 test_case_id: str = None,
                 depends_on: List[str] = None,
                 escape_html: bool = False,
                 points: int = 1,
                 hint=None):

        # Because I cannot figure out how to resolve circular references
        expected_type = "<class 'dbacademy_helper.tests.test_suite_class.TestSuite'>"
        assert str(type(suite)) == expected_type, f"Expected the parameter \"suite\" to be of type TestSuite, found {type(suite)}."

        assert type(description) == str, f"Expected the parameter \"description\" to be of type str, found {type(description)}."

        if test_case_id is None:
            TestCase._LAST_ID += 1
            test_case_id = str(TestCase._LAST_ID)

        self.test_case_id = f"{suite.name}-{test_case_id}"

        self.hint = hint
        self.points = points
        self.escape_html = escape_html
        self.description = description
        self.test_function = test_function
        self.actual_value = actual_value

        depends_on = depends_on or [suite.last_test_id()]
        self.depends_on = depends_on if type(depends_on) is list else [depends_on]

    def update_hint(self):
        self.hint = self.hint.replace("[[ACTUAL_VALUE]]", str(self.actual_value))
        self.hint = self.hint.replace("[[LEN_ACTUAL_VALUE]]", str(len(self.actual_value)))
