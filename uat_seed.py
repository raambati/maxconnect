"""
uat_seed.py - load and keep fresh TEST data for the Supply calculation in a
NON-PRODUCTION environment.

Everything it writes carries   seedTag = "uat-seed"   so it can be found and
removed, and so refresh/clean never touch real or synced items.

Commands (nothing is written unless --apply is given):

  python3 uat_seed.py seed    scenarios.json [--apply]
  python3 uat_seed.py refresh scenarios.json [--every 15] [--minutes 60] [--apply]
  python3 uat_seed.py clean   scenarios.json [--apply]

Table names default to the dev names; override with flags or environment:
  --queue-state  QUEUE_STATE_TABLE_NAME     (key: instanceId, queueId)
  --priority     PRIORITY_SCORES_TABLE_NAME (key: PK, SK)
  --config       CONFIG_TABLE_NAME          (key: PK, SK)
  --agent-state  AGENT_STATE_TABLE_NAME     (only if "agents" are listed)

scenarios.json (see uat_seed_scenarios.json):
  queues:   [{instanceId, queueId, queueName, contactsInQueue,
              oldestContactAgeSeconds, channel (default VOICE),
              snapshotAgeSeconds (default 0 = fresh; use e.g. 600 to make it
              stale and keep it stale), priorityScore, mandatoryPriority}]
  teams:    [{instanceId, teamName, headcount, flexFraction,
              resourceStatus, flexedOut, ...}]
  diversionLimits: {"Comfortable": 0.5, ...}   (optional)
  agents:   [ full agent-state items, written as-is ]  (optional)

refresh re-stamps snapshotTime on every seeded queue each cycle (now minus
snapshotAgeSeconds) and adds the elapsed time to oldestContactAgeSeconds when
contactsInQueue > 0, so waits keep growing like real ones.

It refuses table names containing "prod" or "live" unless --allow-prod-names.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

TAG = "uat-seed"
REGION = os.environ.get("AWS_REGION", "eu-west-2")


def now_iso(offset_seconds=0):
    t = datetime.now(timezone.utc) - timedelta(seconds=offset_seconds)
    return t.isoformat()


def to_dynamo(x):
    return json.loads(json.dumps(x), parse_float=Decimal)


def guard(names, allow):
    bad = [n for n in names if n and any(w in n.lower() for w in ("prod", "live"))]
    if bad and not allow:
        sys.exit("Refusing: table name looks like production: %s (use --allow-prod-names "
                 "only if you are sure)" % ", ".join(bad))


def queue_item(q, started=None):
    age = q.get("snapshotAgeSeconds", 0)
    return {
        "instanceId": q["instanceId"],
        "queueId": q["queueId"],
        "queueName": q["queueName"],
        "channel": q.get("channel", "VOICE"),
        "contactsInQueue": q.get("contactsInQueue", 0),
        "oldestContactAgeSeconds": q.get("oldestContactAgeSeconds", 0),
        "snapshotTime": now_iso(age),
        "seedTag": TAG,
    }


def seed(cfg, args, t):
    plan = []
    for q in cfg.get("queues", []):
        plan.append((t["queue"], queue_item(q), "queue-state %s / %s" % (q["queueName"], q["instanceId"][:8])))
        if "priorityScore" in q:
            plan.append((t["priority"], {
                "PK": "QUEUE#%s" % q["queueName"], "SK": "PRIORITY_SCORE",
                "totalScore": q["priorityScore"],
                "mandatoryPriority": bool(q.get("mandatoryPriority", False)),
                "seedTag": TAG}, "priority %s = %s" % (q["queueName"], q["priorityScore"])))
    for team in cfg.get("teams", []):
        item = dict(team)
        item.update({"PK": "TEAM#%s" % team["instanceId"], "SK": "CONFIG", "seedTag": TAG})
        plan.append((t["config"], item, "team config %s" % team.get("teamName", team["instanceId"][:8])))
    if cfg.get("diversionLimits"):
        plan.append((t["config"], {"PK": "SUPPLY", "SK": "DIVERSION_LIMITS",
                                   "limits": cfg["diversionLimits"], "seedTag": TAG}, "diversion limits"))
    for a in cfg.get("agents", []):
        item = dict(a)
        item["seedTag"] = TAG
        plan.append((t["agent"], item, "agent %s" % a.get("Username", "?")))

    for table, item, label in plan:
        print(("WRITE " if args.apply else "would write ") + label)
        if args.apply:
            table.put_item(Item=to_dynamo(item))
    print("%d item(s) %s." % (len(plan), "written" if args.apply else "planned (add --apply)"))


def refresh(cfg, args, t):
    queues = cfg.get("queues", [])
    started = time.time()
    last = started
    end = started + args.minutes * 60
    grown = {i: q.get("oldestContactAgeSeconds", 0) for i, q in enumerate(queues)}
    print("Refreshing %d queue(s) every %ss for %s min%s" % (
        len(queues), args.every, args.minutes, "" if args.apply else " (DRY - add --apply)"))
    while time.time() < end:
        now = time.time()
        step = now - last
        last = now
        for i, q in enumerate(queues):
            if q.get("contactsInQueue", 0) > 0:
                grown[i] += step
            item = queue_item(q)
            item["oldestContactAgeSeconds"] = int(grown[i]) if q.get("contactsInQueue", 0) > 0 else 0
            if args.apply:
                t["queue"].put_item(Item=to_dynamo(item))
        print("%s refreshed %d queue(s)" % (datetime.now().strftime("%H:%M:%S"), len(queues)))
        time.sleep(args.every)


def clean(cfg, args, t):
    n = 0
    for table, keyfields in ((t["queue"], ("instanceId", "queueId")), (t["priority"], ("PK", "SK")),
                             (t["config"], ("PK", "SK")), (t["agent"], None)):
        if table is None or keyfields is None:
            continue
        kwargs = {"FilterExpression": "seedTag = :t", "ExpressionAttributeValues": {":t": TAG}}
        while True:
            resp = table.scan(**kwargs)
            for it in resp.get("Items", []):
                key = {k: it[k] for k in keyfields}
                print(("DELETE " if args.apply else "would delete ") + "%s %s" % (table.name, key))
                if args.apply:
                    table.delete_item(Key=key)
                n += 1
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    print("%d seeded item(s) %s. (Agent items are not auto-deleted; remove by hand.)" % (
        n, "deleted" if args.apply else "found (add --apply)"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["seed", "refresh", "clean"])
    p.add_argument("scenarios")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--every", type=float, default=15)
    p.add_argument("--minutes", type=float, default=60)
    p.add_argument("--allow-prod-names", action="store_true")
    p.add_argument("--queue-state", default=os.environ.get("QUEUE_STATE_TABLE_NAME", "macp-uk-mgmt-dev-queue-state"))
    p.add_argument("--priority", default=os.environ.get("PRIORITY_SCORES_TABLE_NAME", "mc-dev-priority-scores"))
    p.add_argument("--config", default=os.environ.get("CONFIG_TABLE_NAME", "mc-dev-config"))
    p.add_argument("--agent-state", default=os.environ.get("AGENT_STATE_TABLE_NAME"))
    args = p.parse_args()

    guard([args.queue_state, args.priority, args.config, args.agent_state], args.allow_prod_names)
    with open(args.scenarios, encoding="utf-8") as f:
        cfg = json.load(f)

    ddb = boto3.resource("dynamodb", region_name=REGION)
    t = {"queue": ddb.Table(args.queue_state), "priority": ddb.Table(args.priority),
         "config": ddb.Table(args.config), "agent": ddb.Table(args.agent_state) if args.agent_state else None}
    if cfg.get("agents") and not t["agent"]:
        sys.exit("scenarios list agents but no --agent-state table given")

    {"seed": seed, "refresh": refresh, "clean": clean}[args.command](cfg, args, t)


if __name__ == "__main__":
    main()
