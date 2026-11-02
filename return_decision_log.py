"""
return_decision_log - read-only endpoint for the Management Console Decision Log page.

Called through the ALB (GET). Returns decisions written by supply_calc, newest first.

Query string parameters
  email        agent email. When given, an efficient Query on the agent's history is used.
  decision     MOVE or REMAIN (optional filter)
  since        ISO timestamp, only decisions at or after it (email mode: applied on the key;
               all-agents mode: applied as a filter)
  until        ISO timestamp, only decisions at or before it
  limit        1-200, default 50
  nextToken    value returned by a previous call (email mode only)

Without `email` the function scans the table (capped at MAX_SCAN_ITEMS) and sorts by decidedAt.
That is fine for dev volumes; for production add a GSI (see notes) and switch to a Query.

Response 200:
  {"items": [...], "count": n, "nextToken": "..." | null, "truncated": bool, "mode": "agent" | "all"}
Errors: 400 bad parameter, 405 wrong method, 500 unexpected.
Only reads. It never writes.
"""
import base64
import json
import logging
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE = boto3.resource("dynamodb").Table(os.environ["DECISION_LOG_TABLE_NAME"])
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")
DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_SCAN_ITEMS = int(os.environ.get("MAX_SCAN_ITEMS", "2000"))


def _json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    if isinstance(o, set):
        return sorted(o)
    raise TypeError(type(o))


def _response(status_code, body):
    text = {200: "OK", 204: "No Content", 400: "Bad Request", 405: "Method Not Allowed",
            500: "Internal Server Error"}.get(status_code, "Error")
    return {
        "statusCode": status_code,
        "statusDescription": "%d %s" % (status_code, text),
        "isBase64Encoded": False,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Access-Control-Allow-Methods": "GET,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Cache-Control": "no-store",
        },
        "body": "" if body is None else json.dumps(body, default=_json_default),
    }


class BadRequest(Exception):
    pass


def _params(event):
    p = dict(event.get("queryStringParameters") or {})
    # ALB multi-value form
    for k, v in (event.get("multiValueQueryStringParameters") or {}).items():
        if k not in p and v:
            p[k] = v[-1]
    return p


def _limit(p):
    raw = p.get("limit")
    if raw in (None, ""):
        return DEFAULT_LIMIT
    try:
        n = int(raw)
    except ValueError:
        raise BadRequest("limit must be a whole number")
    if n < 1 or n > MAX_LIMIT:
        raise BadRequest("limit must be between 1 and %d" % MAX_LIMIT)
    return n


def _decision_filter(p):
    d = p.get("decision")
    if not d:
        return None
    d = d.strip().upper()
    if d not in ("MOVE", "REMAIN"):
        raise BadRequest("decision must be MOVE or REMAIN")
    return d


def _encode(key):
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()


def _decode(token):
    try:
        key = json.loads(base64.urlsafe_b64decode(token.encode()).decode())
        if not isinstance(key, dict) or set(key) != {"PK", "SK"}:
            raise ValueError
        return key
    except Exception:
        raise BadRequest("nextToken is not valid")


def query_agent(email, p, limit):
    pk = "AGENT#%s" % email.strip().lower()
    since = p.get("since")
    until = p.get("until")
    if since or until:
        lo = "DECISION#%s" % (since or "")
        hi = "DECISION#%s~" % (until or "9999")
        cond = Key("PK").eq(pk) & Key("SK").between(lo, hi)
    else:
        cond = Key("PK").eq(pk) & Key("SK").begins_with("DECISION#")
    kwargs = {"KeyConditionExpression": cond, "ScanIndexForward": False}
    decision = _decision_filter(p)
    if decision:
        kwargs["FilterExpression"] = Attr("decision").eq(decision)
    if p.get("nextToken"):
        kwargs["ExclusiveStartKey"] = _decode(p["nextToken"])

    items, last_key = [], None
    # a filter can return fewer than `limit` per page, so keep paging until full
    while True:
        resp = TABLE.query(**kwargs)
        for it in resp.get("Items", []):
            items.append(it)
            if len(items) == limit:
                last_key = {"PK": it["PK"], "SK": it["SK"]}
                break
        if len(items) == limit:
            break
        if "LastEvaluatedKey" not in resp:
            last_key = None
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return {"items": items, "count": len(items),
            "nextToken": _encode(last_key) if last_key else None,
            "truncated": False, "mode": "agent"}


def scan_all(p, limit):
    flt = Attr("SK").begins_with("DECISION#")
    decision = _decision_filter(p)
    if decision:
        flt = flt & Attr("decision").eq(decision)
    if p.get("since"):
        flt = flt & Attr("decidedAt").gte(p["since"])
    if p.get("until"):
        flt = flt & Attr("decidedAt").lte(p["until"])
    kwargs = {"FilterExpression": flt}
    items, truncated = [], False
    while True:
        resp = TABLE.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if len(items) >= MAX_SCAN_ITEMS:
            truncated = True
            break
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    items.sort(key=lambda i: str(i.get("decidedAt", "")), reverse=True)
    if len(items) > limit:
        truncated = True
    items = items[:limit]
    return {"items": items, "count": len(items), "nextToken": None,
            "truncated": truncated, "mode": "all"}


def lambda_handler(event, context):
    try:
        method = (event.get("httpMethod") or "GET").upper()
        if method == "OPTIONS":
            return _response(204, None)
        if method != "GET":
            return _response(405, {"error": "only GET is supported"})
        p = _params(event)
        limit = _limit(p)
        email = (p.get("email") or "").strip()
        result = query_agent(email, p, limit) if email else scan_all(p, limit)
        return _response(200, result)
    except BadRequest as exc:
        return _response(400, {"error": str(exc)})
    except Exception:
        logger.exception("decision log read failed")
        return _response(500, {"error": "internal error"})
