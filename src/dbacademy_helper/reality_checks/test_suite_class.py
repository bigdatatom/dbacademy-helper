from typing import List, Callable, Iterable, Any

class TestSuite(object):
    import pyspark
    from pyspark.sql import DataFrame, Row
    from dbacademy_helper.reality_checks import lazy_property
    from dbacademy_helper.reality_checks.test_result_class import TestResult
    from dbacademy_helper.reality_checks.test_case_class import TestCase

    def __init__(self, name) -> None:
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        self.name = name
        self.ids = set()
        self.test_cases: List[TestCase] = list()

    @lazy_property
    def test_results(self) -> List[TestResult]:
        return self.run_tests()

    def run_tests(self) -> List[TestResult]:
        from dbacademy_helper.reality_checks.test_result_class import TestResult
        from dbacademy_helper.reality_checks.test_results_aggregator_class import TestResultsAggregator

        failed_tests = set()
        test_results = list()

        print("-"*80)

        for test in self.test_cases:
            skip = any(test_id in failed_tests for test_id in test.depends_on)
            result = TestResult(test, skip)

            if not result.passed and test.test_case_id is not None:
                failed_tests.add(test.test_case_id)

            test_results.append(result)
            TestResultsAggregator.update(result)

        return test_results

    def _display(self, css_class: str = "results") -> None:
        from html import escape
        from dbacademy_gems import dbgems
        from dbacademy_helper.reality_checks import _TEST_RESULTS_STYLE
        lines = [_TEST_RESULTS_STYLE,
                 "<table class='" + css_class + "'>",
                 "  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>"]

        for result in self.test_results:
            description_html = escape(str(result.test.description)) if result.test.escape_html else str(result.test.description)
            lines.append(f"<tr>")
            lines.append(f"  <td class='points'>{str(result.points)}</td>")
            lines.append(f"  <td class='test'>")
            lines.append(f"    {description_html}")

            if result.status == "failed" and result.test.hint:
                lines.append(f"  <div class='note'>Hint: {escape(str(result.test.hint))}</div>")

            if result.message:
                lines.append(f"    <hr/>")
                lines.append(f"    <div class='message'>{str(result.message)}</div>")

            lines.append(f"  </td>")
            lines.append(f"  <td class='result {result.status}'></td>")
            lines.append(f"</tr>")

        lines.append("  <caption class='points'>Score: " + str(self.score) + "</caption>")
        lines.append("</table>")
        html = "\n".join(lines)
        dbgems.display_html(html)

    def display_results(self) -> None:
        self._display("results")

    def grade(self) -> int:
        self._display("grade")
        return self.score()

    @lazy_property
    def score(self) -> int:
        return sum(map(lambda result: result.points, self.test_results))

    @lazy_property
    def max_score(self) -> int:
        return sum(map(lambda result: result.test.points, self.test_results))

    @lazy_property
    def percentage(self) -> int:
        return 0 if self.max_score == 0 else int(100.0 * self.score() / self.max_score)

    @lazy_property
    def passed(self) -> bool:
        return self.percentage == 100

    def last_test_id(self) -> bool:
        return "-n/a-" if len(self.test_cases) == 0 else self.test_cases[-1].test_case_id

    def add_test(self, test_case: TestCase):
        assert test_case.test_case_id is not None, "The test_case_id must be specified"
        assert test_case.test_case_id not in self.ids, f"Duplicate test case id: {test_case.test_case_id}"

        self.test_cases.append(test_case)
        self.ids.add(test_case.test_case_id)
        return self

    def test(self, test_function: Callable[[], Any], *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      hint=hint,
                                      test_function=test_function))

    def test_equals(self, value_a, value_b, *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      hint=hint,
                                      test_function=lambda: value_a == value_b))

    def test_is_none(self, value: Any, *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      hint=hint,
                                      test_function=lambda: value is None))

    def test_not_none(self, value: Any, *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False, hint=None):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      hint=hint,
                                      test_function=lambda: value is not None))

    def fail_pre_req(self, *, test_case_id: str, e: Exception, depends_on: Iterable[str] = None):
        self.fail(test_case_id=test_case_id,
                  points=1,
                  depends_on=depends_on,
                  escape_html=False,
                  description=f"""<div>Execute prerequisites.</div><div style='max-width: 1024px; overflow-x:auto'>{e}</div>""")

    def fail(self, *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      test_function=lambda: False))

    def test_floats(self, value_a, value_b, *, test_case_id: str = None, description: str = None, tolerance=0.01, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      test_function=lambda: self.compare_floats(value_a, value_b, tolerance)))

    def test_rows(self, row_a: pyspark.sql.Row, row_b: pyspark.sql.Row, *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      test_function=lambda: self.compare_rows(row_a, row_b)))

    def test_data_frames(self, df_a: pyspark.sql.DataFrame, df_b: pyspark.sql.DataFrame, *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      test_function=lambda: self.compare_data_frames(df_a, df_b)))

    def test_contains(self, value: Any, list_of_values: List[Any], *, test_case_id: str = None, description: str = None, points: int = 1, depends_on: Iterable[str] = None, escape_html: bool = False):
        from dbacademy_helper.reality_checks.test_case_class import TestCase

        return self.add_test(TestCase(suite=self,
                                      test_case_id=test_case_id,
                                      description=description,
                                      depends_on=depends_on,
                                      escape_html=escape_html,
                                      points=points,
                                      test_function=lambda: value in list_of_values))

    @staticmethod
    def compare_floats(value_a: float, value_b: float, tolerance: float = 0.01):
        try:
            if value_a is None and value_b is None: return True
            else: return abs(float(value_a) - float(value_b)) <= tolerance
        except:
            return False

    @staticmethod
    def compare_rows(row_a: pyspark.sql.Row, row_b: pyspark.sql.Row):
        if row_a is None and row_b is None: return True
        elif row_a is None or row_b is None: return False

        return row_a.asDict() == row_b.asDict()

    @staticmethod
    def compare_schemas(schema_a: pyspark.sql.types.StructType, schema_b: pyspark.sql.types.StructType, test_column_order: bool):
        from pyspark.sql.types import StructField

        if schema_a is None and schema_b is None: return True
        if schema_a is None or schema_b is None: return False

        sch_a = [StructField(s.name, s.dataType, True) for s in schema_a]
        sch_b = [StructField(s.name, s.dataType, True) for s in schema_b]

        if test_column_order:
            return [sch_a] == [sch_b]
        else:
            return set(sch_a) == set(sch_b)

    @staticmethod
    def compare_data_frames(df_a: DataFrame, df_b: DataFrame):
        from functools import reduce

        if df_a is None and df_b is None:
            return True
        else:
            n = df_a.count()

            if n != df_b.count():
                return False

            kv1 = df_a.rdd.zipWithIndex().map(lambda t: (t[1], t[0])).collectAsMap()
            kv2 = df_b.rdd.zipWithIndex().map(lambda t: (t[1], t[0])).collectAsMap()

            kv12 = [kv1, kv2]
            d = {}

            for k in kv1.keys():
                d[k] = tuple(d[k] for d in kv12)

            return reduce(lambda a, b: a and b, [TestSuite.compare_rows(rowTuple[0], rowTuple[1]) for rowTuple in d.values()])

    @staticmethod
    def compare_row(row_a: Row, row_b: Row):
        if row_a is None and row_b is None: return True
        elif row_a is None or row_b is None: return False

        return row_a.asDict() == row_b.asDict()
