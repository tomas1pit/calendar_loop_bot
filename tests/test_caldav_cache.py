import os
import time

import bot


class DummyClient:
    def __init__(self):
        self.closed = False


class DummyPrincipal:
    def __init__(self):
        pass


def test_get_caldav_client_caches(monkeypatch, tmp_path):
    # Prepare fake caldav module objects
    calls = {"count": 0}

    class FakeDAVClient:
        def __init__(self, url, username=None, password=None):
            calls["count"] += 1
            self.url = url
            self.username = username
            self.password = password
            self.session = None

    class FakePrincipal:
        def __init__(self, client=None, url=None):
            self.client = client
            self.url = url

    monkeypatch.setattr(bot, "caldav", type("m", (), {"DAVClient": FakeDAVClient, "Principal": FakePrincipal}))

    email = "user@example.com"
    pwd = "p"
    # Clear cache
    bot._caldav_client_cache.clear()
    c1, p1 = bot.get_caldav_client(email, pwd)
    c2, p2 = bot.get_caldav_client(email, pwd)
    assert calls["count"] == 1
    assert c1 is c2
    assert p1 is p2

    # Simulate expiry
    bot._caldav_client_cache[email] = (c1, p1, time.time() - (bot.CALDAV_CLIENT_TTL + 10))
    c3, p3 = bot.get_caldav_client(email, pwd)
    assert calls["count"] == 2
    assert c3 is not c1
