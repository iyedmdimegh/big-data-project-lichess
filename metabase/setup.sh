#!/usr/bin/env bash
# Idempotent Metabase bootstrap:
#   1. Read admin creds + Postgres connection from .env
#   2. Hit /api/session/properties for the setup-token
#   3. POST /api/setup to create the admin user and add the ChessDB datasource
#   4. If setup was already done, log and exit 0
#
# Run from the repo root:
#   bash metabase/setup.sh

set -euo pipefail

# Parse .env without `source` — values like `ES_JAVA_OPTS=-Xms512m -Xmx512m`
# trip bash. We only need a fixed allow-list of keys.
load_var() {
    local key="$1"
    grep -E "^${key}=" .env | head -1 | sed -E "s/^${key}=//"
}

METABASE_PORT=$(load_var METABASE_PORT)
METABASE_ADMIN_EMAIL=$(load_var METABASE_ADMIN_EMAIL)
METABASE_ADMIN_PASSWORD=$(load_var METABASE_ADMIN_PASSWORD)
METABASE_ADMIN_FIRSTNAME=$(load_var METABASE_ADMIN_FIRSTNAME)
METABASE_ADMIN_LASTNAME=$(load_var METABASE_ADMIN_LASTNAME)
METABASE_SITE_NAME=$(load_var METABASE_SITE_NAME)
POSTGRES_USER=$(load_var POSTGRES_USER)
POSTGRES_PASSWORD=$(load_var POSTGRES_PASSWORD)
POSTGRES_DB=$(load_var POSTGRES_DB)

export METABASE_ADMIN_EMAIL METABASE_ADMIN_PASSWORD METABASE_ADMIN_FIRSTNAME \
       METABASE_ADMIN_LASTNAME METABASE_SITE_NAME \
       POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

MB_URL="http://localhost:${METABASE_PORT:-3003}"

echo "[metabase] checking ${MB_URL}/api/health"
for i in $(seq 1 30); do
    if curl -fsS "${MB_URL}/api/health" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
curl -fsS "${MB_URL}/api/health" >/dev/null || { echo "[metabase] not reachable"; exit 1; }

PROPS_JSON=$(curl -fsS "${MB_URL}/api/session/properties")
HAS_SETUP=$(echo "$PROPS_JSON" | python -c "import sys, json; d = json.load(sys.stdin); print(d.get('has-user-setup', False))")

if [ "$HAS_SETUP" = "True" ]; then
    echo "[metabase] admin user already exists — checking ChessDB datasource…"
    SESSION=$(curl -fsS -X POST "${MB_URL}/api/session" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"${METABASE_ADMIN_EMAIL}\",\"password\":\"${METABASE_ADMIN_PASSWORD}\"}" \
        | python -c "import sys, json; print(json.load(sys.stdin).get('id',''))")
    if [ -z "$SESSION" ]; then
        echo "[metabase] login with credentials from .env failed. Did someone set up Metabase with different creds? Reset by removing the metabase-db volume."
        exit 1
    fi
    HAS_CHESSDB=$(curl -fsS -H "X-Metabase-Session: $SESSION" "${MB_URL}/api/database" \
        | python -c "import sys, json; print(any(db['name']=='ChessDB' for db in json.load(sys.stdin)['data']))")
    if [ "$HAS_CHESSDB" = "True" ]; then
        echo "[metabase] already initialised; ChessDB datasource present. Log in at ${MB_URL} with ${METABASE_ADMIN_EMAIL}"
        exit 0
    fi
    echo "[metabase] admin exists but ChessDB datasource missing — adding it…"
    curl -fsS -X POST "${MB_URL}/api/database" \
        -H "X-Metabase-Session: $SESSION" \
        -H "Content-Type: application/json" \
        -d "{
            \"engine\":\"postgres\",
            \"name\":\"ChessDB\",
            \"details\":{
                \"host\":\"postgres\",\"port\":5432,
                \"dbname\":\"${POSTGRES_DB}\",
                \"user\":\"${POSTGRES_USER}\",
                \"password\":\"${POSTGRES_PASSWORD}\",
                \"ssl\":false,\"tunnel-enabled\":false
            },
            \"is_full_sync\":true,\"is_on_demand\":false
        }" >/dev/null
    echo "[metabase] ChessDB datasource added. Log in at ${MB_URL}"
    exit 0
fi

TOKEN=$(echo "$PROPS_JSON" | python -c "import sys, json; d = json.load(sys.stdin); print(d.get('setup-token') or '')")
if [ -z "$TOKEN" ]; then
    echo "[metabase] no setup-token returned and no user-setup flag — restart Metabase or use the web wizard at ${MB_URL}"
    exit 1
fi

echo "[metabase] running setup with token=${TOKEN:0:8}…"
export TOKEN

BODY=$(python <<PY
import json, os
print(json.dumps({
    "token": os.environ["TOKEN"],
    "user": {
        "first_name": os.environ["METABASE_ADMIN_FIRSTNAME"],
        "last_name":  os.environ["METABASE_ADMIN_LASTNAME"],
        "email":      os.environ["METABASE_ADMIN_EMAIL"],
        "password":   os.environ["METABASE_ADMIN_PASSWORD"],
        "site_name":  os.environ["METABASE_SITE_NAME"],
    },
    "prefs": {
        "site_name":  os.environ["METABASE_SITE_NAME"],
        "allow_tracking": False,
    },
    "database": {
        "engine": "postgres",
        "name":   "ChessDB",
        "details": {
            "host":         "postgres",
            "port":         5432,
            "dbname":       os.environ["POSTGRES_DB"],
            "user":         os.environ["POSTGRES_USER"],
            "password":     os.environ["POSTGRES_PASSWORD"],
            "ssl":          False,
            "tunnel-enabled": False,
        },
        "is_full_sync": True,
        "is_on_demand": False,
    },
}))
PY
)

export TOKEN

RESP=$(curl -s -o ./mb_setup_resp.tmp -w "%{http_code}" \
    -X POST "${MB_URL}/api/setup" \
    -H "Content-Type: application/json" \
    -d "$BODY")

if [ "$RESP" != "200" ]; then
    echo "[metabase] setup failed (HTTP $RESP):"
    cat ./mb_setup_resp.tmp
    exit 1
fi

SESSION=$(python -c "import json; print(json.load(open('./mb_setup_resp.tmp')).get('id',''))")
echo "[metabase] setup OK. Session token=${SESSION:0:8}…"
echo "[metabase] log in at ${MB_URL} with ${METABASE_ADMIN_EMAIL}"
