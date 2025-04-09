# "pytest" is a popular Python testing framework that simplifies the process of writing and running tests.
# Below is an example that demonstrates key pytest functionalities, including test cases, fixtures,
# parameterization, assertions, and running tests.
#
# To run this test:
# $ pytest pytest_mod.py
#
# Additional useful pytest arguments:
#
# -v: Verbose mode.
# -k <expression>: Run tests matching a name/expression.
# -m <marker>: Run tests with a specific marker.
# --maxfail=n: Stop after n failures.
# --disable-warnings: Suppress warnings.
import pytest
from unittest.mock import MagicMock


class TestMathOperations:

    def test_multiple_assertions(self):
        assert 2 + 2 == 4  # Standard equality assertion
        assert "abc" in "abcdef"  # Membership assertion
        assert [1, 2] == [1, 2]  # List equality
        assert 2 != 3  # Not equal assertion

    # Fixtures allow you to set up reusable components that tests can use.
    @pytest.fixture
    def sample_data(self):
        return {"name": "Alice", "age": 30}

    def test_fixture(self, sample_data):
        assert sample_data["name"] == "Alice"
        assert sample_data["age"] == 30

    # You can run the same test multiple times with different sets of input data using "@pytest.mark.parametrize".
    @pytest.mark.parametrize("a, b, expected", [
        (2, 3, 5),
        (1, 5, 6),
        (0, 0, 0),
    ])
    def test_parameterization(self, a, b, expected):
        assert a + b == expected

    @staticmethod
    def divide(a, b):
        if b == 0:
            raise ZeroDivisionError("Division by zero is not allowed")
        return a / b

    # You can use "pytest.raises" to test for expected exceptions
    def test_divide(self):
        with pytest.raises(ZeroDivisionError, match="Division by zero is not allowed"):
            self.divide(4, 0)

    def hello_world(self):
        print("Hello, World!")

    # Pytest allows you to verify printed or logged output using the "capsys" fixture.
    def test_capsys(self, capsys):
        self.hello_world()
        captured = capsys.readouterr()
        assert captured.out == "Hello, World!\n"

    @staticmethod
    def fetch_data(api):
        return api.get("endpoint")

    # For testing functions that require dependency mocking (e.g., database access), you can use the "MagicMock()".
    def test_fetch_data(self):
        mock_api = MagicMock()
        mock_api.get.return_value = {"data": "test"}
        assert self.fetch_data(mock_api) == {"data": "test"}
