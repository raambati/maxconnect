"""
mc-dev-supply-calc  -  PROPOSED design, shadow mode (recommends, never acts).

When an agent becomes available:
  1. Is the agent routable?
  2. Which queues can they support on OTHER instances?   (agent-state, by email)
  3. Which of those have voice contacts waiting?        (queue-state)
  4. Rank them with Daryll's static priority scores     (mc-dev-priority-scores)
        mandatory priority first -> priority score -> oldest wait
  5. Apply guard rails to the top-ranked queue          (mc-dev-config)
  6. Decide MOVE or REMAIN, and write the decision log  (mc-dev-decision-log)

Request body (JSON):
  {
    "email": "arun.prasad@maximusuk.co.uk",
    "instanceId": "<instance the agent just became available in>",
    "dryRun": true,              # optional - true = do not write the decision log
    "assumeAvailable": true      # optional - TEST ONLY: skip the routable check
  }

NOT implemented yet (open items): cooldown, minimum staffing, sustained
pressure, exception triggers, reason codes, tracking of agents already flexed
out (taken from team config for now).
"""

import json
import logging
import math
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

AGENT_STATE_TABLE = dynamodb.Table(os.environ["AGENT_STATE_TABLE_NAME"])
PRIORITY_TABLE = dynamodb.Table(os.environ["PRIORITY_SCORES_TABLE_NAME"])
CONFIG_TABLE = dynamodb.Table(os.environ["CONFIG_TABLE_NAME"])
DECISION_LOG_TABLE = dynamodb.Table(os.environ["DECISION_LOG_TABLE_NAME"])
QUEUE_STATE_TABLE_NAME = os.environ["QUEUE_STATE_TABLE_NAME"]
# Optional: table that maps instanceId -> friendly name (macp-uk-mgmt-<env>-instance-details).
# Without it, decisions simply show the instance ID as the name.
INSTANCE_DETAILS_TABLE_NAME = os.environ.get("INSTANCE_DETAILS_TABLE_NAME", "")

MAX_QUEUE_STATE_AGE_SECONDS = int(os.environ.get("MAX_QUEUE_STATE_AGE_SECONDS", "180"))
# Open item: should Mandatory Priority moves bypass the flexible-capacity cap?
# Fail-safe default is NO.
MANDATORY_BYPASSES_FLEX_CAP = os.environ.get("MANDATORY_BYPASSES_FLEX_CAP", "false").lower() == "true"

ROUTABLE_STATE_TYPE = "ROUTABLE"

# Fallback only - from Daryll's Framework v2 (diversion limits by team status).
# The live values should be loaded from mc-dev-config (PK=SUPPLY, SK=DIVERSION_LIMITS).
DEFAULT_DIVERSION_LIMITS = {
    "Surplus": 0.20,
    "Comfortable": 0.15,
    "Managed": 0.10,
    "High Constraint": 0.05,
    "Critical Constraint": 0.0,
}


# ----------------------------------------------------------------- helpers
def _num(value, default=0.0):
    if value is None:
        return default
    return float(value) if isinstance(value, Decimal) else float(value)


def _as_bool(value, default=False):
    # A string such as "false" must not be treated as True.
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def _to_dynamo(item):
    """Convert floats to Decimal so DynamoDB accepts the item."""
    return json.loads(json.dumps(item, default=_json_default), parse_float=Decimal)


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "statusDescription": f"{status_code} {'OK' if status_code == 200 else 'Error'}",
        "isBase64Encoded": False,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=_json_default),
    }


def _parse_body(event):
    body = event.get("body")
    if isinstance(body, str):
        return json.loads(body) if body.strip() else {}
    if isinstance(body, dict):
        return body
    return event


# -------------------------------------------------------------- data access
def find_agent_records(email):
    """All agent-state records for this email (case-insensitive), across instances."""
    records = []
    kwargs = {"FilterExpression": Attr("Username").exists()}
    while True:
        resp = AGENT_STATE_TABLE.scan(**kwargs)
        for item in resp.get("Items", []):
            if str(item.get("Username", "")).strip().lower() == email:
                records.append(item)
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return records


def build_eligible_queues(records, current_instance_id, home=False):
    """Eligible queues on OTHER instances, de-duplicated (home=True: the current instance only)."""
    seen, queues = set(), []
    for rec in records:
        inst = rec.get("instanceId")
        if (inst == current_instance_id) != home:
            continue  # moving means leaving the current instance
        for q in rec.get("EligibleQueues", []) or []:
            raw = q.get("queueId")
            if not raw:
                continue
            raw = str(raw)
            if "/queue/agent/" in raw:
                continue  # personal agent queue, not a real queue
            # agent-state stores the queue ARN; queue-state keys on the bare UUID
            qid = raw.rsplit("/", 1)[-1]
            key = (inst, qid)
            if not qid or key in seen:
                continue
            seen.add(key)
            queues.append({"instanceId": inst, "queueId": qid, "queueName": q.get("queueName")})
    return queues


def batch_get_queue_states(queues):
    keys = [{"instanceId": q["instanceId"], "queueId": q["queueId"]} for q in queues]
    found = {}
    for i in range(0, len(keys), 100):
        pending = {QUEUE_STATE_TABLE_NAME: {"Keys": keys[i:i + 100]}}
        for _ in range(5):  # retry unprocessed keys a few times
            resp = dynamodb.batch_get_item(RequestItems=pending)
            for item in resp.get("Responses", {}).get(QUEUE_STATE_TABLE_NAME, []):
                found[(item["instanceId"], item["queueId"])] = item
            pending = resp.get("UnprocessedKeys") or {}
            if not pending:
                break
    return found


def get_priority(queue_name):
    # NOTE: priority scores are keyed by queue NAME. Duplicate names across
    # instances (e.g. Quick-Connect-Queue) would collide - see open items.
    resp = PRIORITY_TABLE.get_item(Key={"PK": f"QUEUE#{queue_name}", "SK": "PRIORITY_SCORE"})
    return resp.get("Item")


def load_team_config(instance_id):
    resp = CONFIG_TABLE.get_item(Key={"PK": f"TEAM#{instance_id}", "SK": "CONFIG"})
    return resp.get("Item")


_INSTANCE_NAMES = None


def load_instance_names():
    """instanceId -> friendly name, e.g. 'Bureau (UAT)'. Cached for the life of the container.
    A scan is fine: the table holds a handful of instances. Never fails the decision."""
    global _INSTANCE_NAMES
    if _INSTANCE_NAMES is not None:
        return _INSTANCE_NAMES
    names = {}
    if INSTANCE_DETAILS_TABLE_NAME:
        try:
            table = dynamodb.Table(INSTANCE_DETAILS_TABLE_NAME)
            kwargs = {}
            while True:
                resp = table.scan(**kwargs)
                for it in resp.get("Items", []):
                    if it.get("instanceId") and it.get("instanceName"):
                        names[str(it["instanceId"]).strip()] = str(it["instanceName"]).strip()
                if "LastEvaluatedKey" not in resp:
                    break
                kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        except Exception:
            logger.exception("could not load instance names; showing instance IDs instead")
    _INSTANCE_NAMES = names
    return names


def instance_name(instance_id):
    return load_instance_names().get(str(instance_id), str(instance_id))


def load_diversion_limits():
    resp = CONFIG_TABLE.get_item(Key={"PK": "SUPPLY", "SK": "DIVERSION_LIMITS"})
    item = resp.get("Item")
    if item and isinstance(item.get("limits"), dict):
        return {k: _num(v) for k, v in item["limits"].items()}
    return dict(DEFAULT_DIVERSION_LIMITS)


# ---------------------------------------------------------------- the logic
def _age_seconds(iso_text):
    try:
        ts = datetime.fromisoformat(str(iso_text).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except (ValueError, TypeError):
        return None


def select_candidates(queues, states):
    candidates, excluded = [], {}

    def skip(reason):
        excluded[reason] = excluded.get(reason, 0) + 1

    for q in queues:
        st = states.get((q["instanceId"], q["queueId"]))
        if not st:
            skip("NO_QUEUE_STATE"); continue
        if str(st.get("channel", "")).upper() != "VOICE":
            skip("NOT_VOICE"); continue
        if _num(st.get("contactsInQueue")) <= 0:
            skip("NOBODY_WAITING"); continue
        age = _age_seconds(st.get("snapshotTime"))
        if age is None or age > MAX_QUEUE_STATE_AGE_SECONDS:
            skip("STALE_QUEUE_STATE"); continue
        prio = get_priority(q["queueName"] or st.get("queueName"))
        if not prio:
            skip("NO_PRIORITY_SCORE"); continue
        candidates.append({
            "instanceId": q["instanceId"],
            "instanceName": instance_name(q["instanceId"]),
            "queueId": q["queueId"],
            "queueName": q["queueName"] or st.get("queueName"),
            "contactsWaiting": _num(st.get("contactsInQueue")),
            "oldestWaitSeconds": _num(st.get("oldestContactAgeSeconds")),
            "priorityScore": _num(prio.get("totalScore")),
            "mandatoryPriority": bool(prio.get("mandatoryPriority")),
        })

    # mandatory first, then priority score, then oldest wait
    candidates.sort(key=lambda c: (not c["mandatoryPriority"], -c["priorityScore"], -c["oldestWaitSeconds"]))
    for rank, c in enumerate(candidates, start=1):
        c["rank"] = rank
    return candidates, excluded


def home_pressure(home_queues, states):
    """Home-instance queues the agent can serve that currently have fresh voice contacts waiting."""
    out = []
    for q in home_queues:
        st = states.get((q["instanceId"], q["queueId"]))
        if not st or str(st.get("channel", "")).upper() != "VOICE":
            continue
        if _num(st.get("contactsInQueue")) <= 0:
            continue
        age = _age_seconds(st.get("snapshotTime"))
        if age is None or age > MAX_QUEUE_STATE_AGE_SECONDS:
            continue
        name = q.get("queueName") or st.get("queueName")
        prio = get_priority(name)
        out.append({"instanceId": q["instanceId"],
                    "instanceName": instance_name(q["instanceId"]),
                    "queueName": name,
                    "priorityScore": _num(prio.get("totalScore")) if prio else None,
                    "mandatoryPriority": bool(prio.get("mandatoryPriority")) if prio else False,
                    "contactsWaiting": _num(st.get("contactsInQueue")),
                    "oldestWaitSeconds": _num(st.get("oldestContactAgeSeconds"))})
    return out


def run_guard_rails(routable, top, team, limits, home_pressured=None, considered=0, excluded=None):
    checks = []

    def add(name, measured, required, passed, note=""):
        checks.append({"check": name, "measured": measured, "required": required,
                       "result": "PASS" if passed else "FAIL", "note": note})

    add("Agent is routable", routable, True, routable)
    why = ""
    if top is None:
        why = ("Checked %d queue(s) this agent can support on other instances; none has voice contacts waiting "
               "with a fresh snapshot and a priority score. Reasons: %s" % (
                   considered, ", ".join("%s=%s" % kv for kv in sorted((excluded or {}).items())) or "no queues on profile"))
    add("A queue this agent can support has contacts waiting",
        ("%s (%s)" % (top["queueName"], top.get("instanceName"))) if top else "none",
        "at least one", top is not None, why)
    if not top:
        return checks, None

    # Do not pull an agent away from where they are while there is still pressure there.
    # Stay put if a queue the agent already supports where they are, and that is under pressure, has
    # an equal or higher priority than the queue we would move them to. A lower-priority queue at
    # home does not block a move to a more important one. A home queue with no priority score cannot
    # be compared, so it is treated as blocking (fail-safe: do not move).
    top_key = (top["mandatoryPriority"], top["priorityScore"])
    hp = home_pressured or []
    blocking = [h for h in hp
                if h.get("priorityScore") is None or (h["mandatoryPriority"], h["priorityScore"]) >= top_key]
    lower = [h["queueName"] for h in hp if h not in blocking]
    note = ""
    if blocking:
        note = ("HOME_UNDER_PRESSURE - stay where the agent is: " + ", ".join(
            "%s (score %s)" % (h["queueName"], h["priorityScore"] if h["priorityScore"] is not None else "none")
            for h in blocking) + " is equal or higher priority than %s (score %s)" % (top["queueName"], top["priorityScore"]))
    elif lower:
        note = "Home queue(s) under pressure but lower priority than the target: " + ", ".join(lower)
    add("No equal or higher priority queue under pressure where the agent is now",
        [h["queueName"] for h in blocking], [], not blocking, note)

    if not team:
        add("Home team is configured", False, True, False, "TEAM_NOT_CONFIGURED - fail-safe REMAIN")
        return checks, None
    add("Home team is configured", True, True, True)

    headcount = _num(team.get("headcount"))
    flexed = _num(team.get("flexedOut"))
    flex_fraction = _num(team.get("flexFraction"))
    status = team.get("resourceStatus")
    limit = limits.get(status)
    cap = math.floor(headcount * min(flex_fraction, limit)) if limit is not None else 0

    bypass = top["mandatoryPriority"] and MANDATORY_BYPASSES_FLEX_CAP
    add("Flexible capacity: agents flexed out after this move vs cap",
        flexed + 1, cap, (flexed + 1 <= cap) or bypass,
        f"status={status}, flexFraction={flex_fraction}, diversionLimit={limit}"
        + (", mandatory-priority bypass applied" if bypass else "")
        + "; %s: headcount %s x %s%% (%s) = %s agent(s) may be lent out, %s already out, this move makes %s" % (
            team.get("teamName") or "team", int(headcount),
            round(min(flex_fraction, limit if limit is not None else 0) * 100, 1), status, cap, int(flexed), int(flexed) + 1))
    return checks, cap


def apply_scenario(queues, states, scenario_queues):
    """Test-only: pretend some queues have contacts waiting. Dry runs only.

    Each entry: {"queueName" or "queueId", "contactsInQueue", "oldestContactAgeSeconds"}.
    Matches eligible queues by bare queueId or case-insensitive queueName.
    Returns (applied_count, unmatched_entries)."""
    now_iso = datetime.now(timezone.utc).isoformat()
    applied, unmatched = 0, []
    for entry in scenario_queues:
        want_id = str(entry.get("queueId", "")).rsplit("/", 1)[-1]
        want_name = str(entry.get("queueName", "")).strip().lower()
        hit = False
        for q in queues:
            if (want_id and q["queueId"] == want_id) or (
                    want_name and str(q.get("queueName") or "").strip().lower() == want_name):
                key = (q["instanceId"], q["queueId"])
                st = dict(states.get(key) or {})
                st.update({
                    "instanceId": q["instanceId"],
                    "queueId": q["queueId"],
                    "queueName": q.get("queueName") or st.get("queueName"),
                    "channel": "VOICE",
                    "contactsInQueue": entry.get("contactsInQueue", 1),
                    "oldestContactAgeSeconds": entry.get("oldestContactAgeSeconds", 0),
                    "snapshotTime": now_iso,
                })
                states[key] = st
                applied += 1
                hit = True
        if not hit:
            unmatched.append(entry)
    return applied, unmatched


def _clean_id(value):
    """Trim whitespace and invisible characters (zero-width space etc. from copy and paste)."""
    if value is None:
        return None
    return "".join(ch for ch in str(value) if ch.isprintable() and not ch.isspace()
                   and ch not in "\u200b\u200c\u200d\u2060\ufeff")


def decide(body):
    email = str(body.get("email", "")).strip().lower()
    if not email:
        return 400, {"message": "email is required"}
    dry_run = _as_bool(body.get("dryRun"), False)
    assume_available = _as_bool(body.get("assumeAvailable"), False)
    scenario = body.get("scenario") or {}
    scenario_queues = scenario.get("queues") if isinstance(scenario, dict) else None
    if scenario_queues and not dry_run:
        return 400, {"message": "scenario can only be used with dryRun: true"}

    records = find_agent_records(email)
    if not records:
        return 404, {"message": "No agent found with that email", "email": email}

    current_instance = _clean_id(body.get("instanceId"))
    if not current_instance:
        if len(records) == 1:
            current_instance = records[0]["instanceId"]
        else:
            return 400, {"message": "instanceId is required when the agent has records on several instances",
                         "instances": [r["instanceId"] for r in records]}

    current = next((r for r in records if r.get("instanceId") == current_instance), None)
    if current is None:
        return 400, {"message": "instanceId does not match any instance this agent is on",
                     "instanceId": current_instance,
                     "instances": [r["instanceId"] for r in records]}
    routable = assume_available or (current is not None and current.get("CurrentStateType") == ROUTABLE_STATE_TYPE)

    queues = build_eligible_queues(records, current_instance)
    home_queues = build_eligible_queues(records, current_instance, home=True)
    states = batch_get_queue_states(queues + home_queues) if (queues or home_queues) else {}
    scenario_info = None
    if scenario_queues:
        applied, unmatched = apply_scenario(queues + home_queues, states, scenario_queues)
        scenario_info = {"applied": applied, "unmatched": unmatched}
    candidates, excluded = select_candidates(queues, states)
    top = candidates[0] if candidates else None

    team = load_team_config(current_instance) if top else None
    limits = load_diversion_limits()
    home_pressured = home_pressure(home_queues, states)
    checks, cap = run_guard_rails(routable, top, team, limits, home_pressured,
                                  considered=len(queues), excluded=excluded)

    failed = [c for c in checks if c["result"] == "FAIL"]
    if failed:
        decision, target = "REMAIN", None
        reason = f"Blocked by: {failed[0]['check']}"
    else:
        decision, target = "MOVE", top
        reason = "All checks passed - highest-priority eligible queue with voice contacts waiting"

    now = datetime.now(timezone.utc)
    decision_id = str(uuid.uuid4())
    result = {
        "decisionId": decision_id,
        "decidedAt": now.isoformat(),
        "mode": "SHADOW",
        "actioned": False,
        "decision": decision,
        "reason": reason,
        "email": email,
        "currentInstanceId": current_instance,
        "currentInstanceName": instance_name(current_instance),
        "assumedAvailable": assume_available,
        "target": target,
        # every queue found under pressure: HOME = where the agent is now, OTHER = could move there
        "pressureQueues": (
            [dict(p, location="HOME") for p in home_pressured]
            + [{"instanceId": c["instanceId"], "instanceName": c["instanceName"], "queueName": c["queueName"],
                "contactsWaiting": c["contactsWaiting"], "oldestWaitSeconds": c["oldestWaitSeconds"],
                "priorityScore": c["priorityScore"], "rank": c["rank"], "location": "OTHER_INSTANCE"}
               for c in candidates]),
        "flexCap": cap,
        "guardRails": checks,
        "eligibleQueuesConsidered": len(queues),
        "excludedSummary": excluded,
        "candidates": candidates[:15],
        "dryRun": dry_run,
        "logged": False,
    }
    if scenario_info is not None:
        result["scenario"] = scenario_info
    if dry_run and _as_bool(body.get("listEligibleQueues"), False):
        # Helper for building test scenarios: the exact names/IDs this agent could be moved to.
        result["eligibleQueues"] = sorted(
            ({"instanceId": q["instanceId"], "queueName": q.get("queueName"), "queueId": q["queueId"]}
             for q in queues),
            key=lambda x: (x["instanceId"], str(x["queueName"])))

    if not dry_run:
        # Estate picture at decision time is stored with the decision, so an ops
        # manager can judge "would I have done the same?" afterwards.
        DECISION_LOG_TABLE.put_item(Item=_to_dynamo({
            "PK": f"AGENT#{email}",
            "SK": f"DECISION#{now.isoformat()}#{decision_id}",
            **result,
            "teamConfig": team,
            "diversionLimits": limits,
            "logged": True,
        }))
        result["logged"] = True

    return 200, result


def lambda_handler(event, context):
    try:
        status, body = decide(_parse_body(event))
        return _response(status, body)
    except (ClientError, BotoCoreError) as exc:
        logger.exception("AWS service error")
        return _response(500, {"message": "AWS service error", "error": str(exc)})
    except Exception as exc:
        logger.exception("Supply calculation failed")
        return _response(500, {"message": "Supply calculation failed", "error": str(exc)})
