import pytest
from unittest import mock
from datetime import datetime, timedelta

from freezegun import freeze_time
from target_snowflake.target_snowflake import Expires


now = datetime.utcnow()


class TestExpire:
    @pytest.fixture
    def subject(self):
        return Expires(10)  # expires in 1 second

    @freeze_time(now)
    def test_armed(self, subject):
        assert subject._armed

        # disarming works
        subject.disarm()
        assert not subject._armed
        assert not subject.expired(at=now + timedelta(seconds=1000))

        # arms automatically
        subject.rearm(5)
        assert subject._armed

    @freeze_time(now)
    def test_rearm(self, subject):
        assert not subject.expired()

        # can set the TTL ad-hoc
        subject.rearm(1)
        assert subject.expires_at == (now + timedelta(seconds=1)).timestamp()
        assert subject.expired(at=now + timedelta(seconds=2))

        # or to a certain time
        expires_at = now + timedelta(seconds=10)
        subject.rearm_at(expires_at)
        assert subject.expires_at == expires_at.timestamp()

        # or use the default TTL
        subject.rearm()
        assert subject.expires_at >= (now + timedelta(seconds=10)).timestamp()
