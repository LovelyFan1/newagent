from __future__ import annotations


async def test_register_login_me_flow(client):
    r = await client.post("/api/v1/auth/register", json={"email": "u1@example.com", "password": "Passw0rd!!"})
    assert r.status_code == 200
    body = r.json()
    assert body["code"] == 0
    token = body["data"]["access_token"]
    assert token

    r = await client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    body = r.json()
    assert body["code"] == 0
    assert body["data"]["email"] == "u1@example.com"

    r = await client.post(
        "/api/v1/auth/login",
        data={"username": "u1@example.com", "password": "Passw0rd!!"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["code"] == 0
    assert body["data"]["access_token"]

