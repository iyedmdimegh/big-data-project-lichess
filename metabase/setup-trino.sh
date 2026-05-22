#!/usr/bin/env bash
# Bootstrap the experimental Metabase instance that queries Cassandra via Trino.
# Mirrors metabase/setup.sh in shape but targets a different host/db/instance.
#
# Run from the repo root:
#   bash metabase/setup-trino.sh

set -euo pipefail

load_var() {
    local key="$1"
    grep -E "^${key}=" .env | head -1 | sed -E "s/^${key}=//"
}

METABASE_TRINO_PORT=$(load_var METABASE_TRINO_PORT)
ADMIN_EMAIL=$(load_var METABASE_TRINO_ADMIN_EMAIL)
ADMIN_PASSWORD=$(load_var METABASE_TRINO_ADMIN_PASSWORD)
ADMIN_FIRSTNAME=$(load_var METABASE_ADMIN_FIRSTNAME)
ADMIN_LASTNAME=$(load_var METABASE_ADMIN_LASTNAME)
SITE_NAME=$(load_var METABASE_TRINO_SITE_NAME)

export ADMIN_EMAIL ADMIN_PASSWORD ADMIN_FIRSTNAME ADMIN_LASTNAME SITE_NAME

MB_URL="http://localhost:${METABASE_TRINO_PORT}"

echo "[metabase-trino] checking ${MB_URL}/api/health"
for i in $(seq 1 60); do
    if curl -fsS "${MB_URL}/api/health" >/dev/null 2>&1; then
        break
    fi
    sleep 3
done
curl -fsS "${MB_URL}/api/health" >/dev/null || { echo "[metabase-trino] not reachable"; exit 1; }

PROPS_JSON=$(curl -fsS "${MB_URL}/api/session/properties")
HAS_SETUP=$(echo "$PROPS_JSON" | python -c "import sys, json; d = json.load(sys.stdin); print(d.get('has-user-setup', False))")

if [ "$HAS_SETUP" = "True" ]; then
    echo "[metabase-trino] admin user already exists — checking ChessCassandra datasource…"
    SESSION=$(curl -fsS -X POST "${MB_URL}/api/session" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PASSWORD}\"}" \
        | python -c "import sys, json; print(json.load(sys.stdin).get('id',''))")
    if [ -z "$SESSION" ]; then
        echo "[metabase-trino] login failed. Reset by removing the metabase_trino_db_data volume."
        exit 1
    fi
    HAS_DS=$(curl -fsS -H "X-Metabase-Session: $SESSION" "${MB_URL}/api/database" \
        | python -c "import sys, json; print(any(db['name']=='ChessCassandra' for db in json.load(sys.stdin)['data']))")
    if [ "$HAS_DS" = "True" ]; then
        echo "[metabase-trino] already initialised; ChessCassandra (Trino) datasource present."
        echo "[metabase-trino] log in at ${MB_URL} with ${ADMIN_EMAIL}"
        exit 0
    fi
    echo "[metabase-trino] admin exists but ChessCassandra datasource missing — adding it…"
    RESP=$(curl -s -o ./mb_trino_addds_resp.tmp -w "%{http_code}" -X POST "${MB_URL}/api/database" \
        -H "X-Metabase-Session: $SESSION" \
        -H "Content-Type: application/json" \
        -d "{
            \"engine\":\"presto-jdbc\",
            \"name\":\"ChessCassandra\",
            \"details\":{
                \"host\":\"trino\",
                \"port\":8080,
                \"catalog\":\"cassandra\",
                \"schema\":\"chess\",
                \"user\":\"chess\",
                \"ssl\":false,
                \"tunnel-enabled\":false
            },
            \"is_full_sync\":true,\"is_on_demand\":false
        }")
    if [ "$RESP" != "200" ]; then
        echo "[metabase-trino] add-datasource failed (HTTP $RESP):"
        cat ./mb_trino_addds_resp.tmp
        rm -f ./mb_trino_addds_resp.tmp
        exit 1
    fi
    rm -f ./mb_trino_addds_resp.tmp
    echo "[metabase-trino] ChessCassandra datasource added. Log in at ${MB_URL}"
    exit 0
fi

# First-time setup: create admin + add Trino datasource in one call.
TOKEN=$(echo "$PROPS_JSON" | python -c "import sys, json; d = json.load(sys.stdin); print(d.get('setup-token') or '')")
if [ -z "$TOKEN" ]; then
    echo "[metabase-trino] no setup-token returned. Restart Metabase or use the wizard at ${MB_URL}"
    exit 1
fi

echo "[metabase-trino] running setup with token=${TOKEN:0:8}…"
export TOKEN

BODY=$(python <<'PY'
import json, os
print(json.dumps({
    "token": os.environ["TOKEN"],
    "user": {
        "first_name": os.environ["ADMIN_FIRSTNAME"],
        "last_name":  os.environ["ADMIN_LASTNAME"],
        "email":      os.environ["ADMIN_EMAIL"],
        "password":   os.environ["ADMIN_PASSWORD"],
        "site_name":  os.environ["SITE_NAME"],
    },
    "prefs": {
        "site_name": os.environ["SITE_NAME"],
        "allow_tracking": False,
    },
    "database": {
        "engine": "presto-jdbc",
        "name":   "ChessCassandra",
        "details": {
            "host":           "trino",
            "port":           8080,
            "catalog":        "cassandra",
            "schema":         "chess",
            "user":           "chess",
            "ssl":            False,
            "tunnel-enabled": False,
        },
        "is_full_sync": True,
        "is_on_demand": False,
    },
}))
PY
)

RESP=$(curl -s -o ./mb_trino_setup_resp.tmp -w "%{http_code}" \
    -X POST "${MB_URL}/api/setup" \
    -H "Content-Type: application/json" \
    -d "$BODY")

if [ "$RESP" != "200" ]; then
    echo "[metabase-trino] setup failed (HTTP $RESP):"
    cat ./mb_trino_setup_resp.tmp
    rm -f ./mb_trino_setup_resp.tmp
    exit 1
fi

SESSION=$(python -c "import json; print(json.load(open('./mb_trino_setup_resp.tmp')).get('id',''))")
rm -f ./mb_trino_setup_resp.tmp
echo "[metabase-trino] setup OK. Session=${SESSION:0:8}…"
echo "[metabase-trino] log in at ${MB_URL} with ${ADMIN_EMAIL}"
