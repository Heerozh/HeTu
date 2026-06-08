import pytest

import hetu
from hetu.endpoint.guard import ClientReject
from hetu.endpoint.response import RejectResponse


def test_client_reject_carries_code_and_reason():
    e = ClientReject("RATE_LIMITED", "太快了")
    assert e.code == "RATE_LIMITED"
    assert e.reason == "太快了"
    assert isinstance(e, Exception)


def test_client_reject_defaults():
    e = ClientReject()
    assert e.code == "REJECTED"
    assert e.reason is None


def test_reject_response_carries_code():
    r = RejectResponse("RATE_LIMITED")
    assert r.code == "RATE_LIMITED"
    assert r.reason is None
