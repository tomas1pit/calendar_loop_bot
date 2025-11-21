import os
import json
import responses

import bot


@responses.activate
def test_mm_get_success(monkeypatch):
    monkeypatch.setenv("MATTERMOST_BASE_URL", "https://mm.test")
    monkeypatch.setenv("MATTERMOST_BOT_TOKEN", "tok")
    # Ensure module-level variables match env
    bot.MATTERMOST_BASE_URL = os.getenv("MATTERMOST_BASE_URL")
    bot.MATTERMOST_BOT_TOKEN = os.getenv("MATTERMOST_BOT_TOKEN")

    url = "https://mm.test/api/v4/ping"
    responses.add(responses.GET, url, json={"pong": True}, status=200)

    res = bot.mm_get("/api/v4/ping")
    assert res == {"pong": True}


@responses.activate
def test_mm_post_success(monkeypatch):
    monkeypatch.setenv("MATTERMOST_BASE_URL", "https://mm.test")
    monkeypatch.setenv("MATTERMOST_BOT_TOKEN", "tok")
    bot.MATTERMOST_BASE_URL = os.getenv("MATTERMOST_BASE_URL")
    bot.MATTERMOST_BOT_TOKEN = os.getenv("MATTERMOST_BOT_TOKEN")

    url = "https://mm.test/api/v4/posts"
    payload = {"channel_id": "c1", "message": "hi"}
    responses.add(responses.POST, url, json={"id": "p1", "message": "hi"}, status=201)

    res = bot.mm_post("/api/v4/posts", payload)
    assert res.get("id") == "p1"


@responses.activate
def test_mm_get_retries_on_500(monkeypatch):
    monkeypatch.setenv("MATTERMOST_BASE_URL", "https://mm.test")
    monkeypatch.setenv("MATTERMOST_BOT_TOKEN", "tok")
    bot.MATTERMOST_BASE_URL = os.getenv("MATTERMOST_BASE_URL")
    bot.MATTERMOST_BOT_TOKEN = os.getenv("MATTERMOST_BOT_TOKEN")

    url = "https://mm.test/api/v4/ping"
    # First response: 500, second: 200
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, json={"pong": True}, status=200)

    res = bot.mm_get("/api/v4/ping")
    assert res == {"pong": True}
