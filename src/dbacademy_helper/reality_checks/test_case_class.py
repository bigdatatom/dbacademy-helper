class TestCase(object):
    from typing import Callable, Any, Iterable

    __slots__ = ('description', 'test_function', 'test_case_id', 'unique_id', 'depends_on', 'escape_html', 'points', 'hint')

    def __init__(self,
                 *,
                 suite,
                 test_function: Callable[[], Any],
                 description: str = None,
                 test_case_id: str = None,
                 depends_on: Iterable[str] = None,
                 escape_html: bool = False,
                 points: int = 1,
                 hint=None):

        from typing import List
        import uuid

        assert str(type(suite)) == "TestSuite", f"Expected the parameter \"suite\" to be of type TestSuite, found {type(suite)}"

        self.test_case_id = f"{suite.name}-{test_case_id}" or f"{suite.name}-{uuid.uuid4()}"
        self.hint = hint
        self.points = points
        self.escape_html = escape_html
        self.description = description
        self.test_function = test_function

        depends_on = depends_on or [suite.last_test_id()]
        self.depends_on = depends_on if type(depends_on) is List else [depends_on]
