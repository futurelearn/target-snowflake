import pytest
from unittest import mock
from datetime import datetime, timedelta

from target_snowflake.target_snowflake import Expires


class TestExpire:
    @pytest.fixture(scope="class")
    def now(self):
        return datetime.utcnow()

    @pytest.fixture
    def subject(self, now):
        return Expires(10, at=now)  # expires in 1 second

    def test_armed(self, subject, now):
        assert subject._armed

        # disarming works
        subject.disarm()
        assert not subject._armed
        assert not subject.expired(at=now + timedelta(seconds=1000))

        # arms automatically
        subject.rearm(5)
        assert subject._armed

    def test_rearm(self, subject, now):
        fixed_time = mock.Mock()
        fixed_time.utcnow.return_value = now

        assert not subject.expired()

        # can set the TTL ad-hoc
        subject.rearm(ttl=1, at=now)
        assert subject.expires_at == (now + timedelta(seconds=1)).timestamp()
        assert subject.expired(at=now + timedelta(seconds=2))

        # or use the default TTL
        subject.rearm(at=now)
        assert subject.expires_at == (now + timedelta(seconds=10)).timestamp()

        subject.rearm(10)
        assert subject.expires_at >= (now + timedelta(seconds=10)).timestamp()
