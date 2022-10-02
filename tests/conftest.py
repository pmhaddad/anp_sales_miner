import datetime

import pytest


@pytest.fixture
def test_time():
    return datetime.datetime(2020, 12, 25, 17, 5, 55)


@pytest.fixture
def patch_datetime_now(monkeypatch, test_time):
    class my_datetime:
        @classmethod
        def now(cls):
            return test_time

    monkeypatch.setattr('datetime.datetime', my_datetime)
