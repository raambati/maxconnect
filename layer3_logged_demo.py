"""
layer3_logged_demo.py - seeds DEV queue-state and demand-counter rows so that a LOGGED
(non-dry-run) supply_calc run for t.mind@maximusuk.co.uk writes a real decision-log record.

  python3 layer3_logged_demo.py show   A                      (prints what would be written)
  python3 layer3_logged_demo.py seed   A --counters <table> --apply
  python3 layer3_logged_demo.py clear  --counters <table> --apply

Cases A-G match the Layer 3 dry-run tests, with call counts recalculated for TODAY (the month cap
is daily cap x today's day of the month). Queue state goes stale after 180 s, so run supply_calc
within about 2 minutes of seeding. Every row carries seedTag = "layer3-demo"; `clear` deletes only
those rows and puts back any real queue-state row that was overwritten (saved in
layer3_demo_backup.json). Refuses table names containing prod/live.
"""
import argparse, json, os, sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo
import boto3

TAG = "layer3-demo"
BACKUP = "layer3_demo_backup.json"
BUREAU = "decdaa49-797c-4783-ab3f-8fa195bfb6b3"
GAMS = "a61e433d-0b3c-46d6-b6cc-2cc9f3f6c6c3"
EMP = (BUREAU, "1aae2706-d332-4fc7-b23c-fe719c9df51f", "Empathy_Tel", "empathy_tel")
GS = (GAMS, "400ead14-2328-4210-a368-25bf9e3138f1", "Gamstop_Online_General_Enquiries_Tel",
      "gamstop_online_general_enquiries_tel")
# case -> (what it shows, Empathy waiting, {queue key: (calls today, calls month to date)})
CASES = {
    "A": ("within both caps: no change", 0, {"gs": (15, 300)}),
    "B": ("month to date 108.6% over: x0.95", 0, {"gs": (15, 645)}),
    "C": ("day 113.6% over: x0.85", 0, {"gs": (25, 300)}),
    "D": ("both over, month worse: x0.5", 0, {"gs": (25, 840)}),
    "E": ("both over, day worse: x0.5", 0, {"gs": (33, 598)}),
    "F": ("ranking flips to GamStop", 3, {"emp": (5, 10), "gs": (1, 1)}),
    "G": ("no demand: Empathy wins on static score", 3, {}),
}


def now_local():
    return datetime.now(ZoneInfo("Europe/London")).date()


def plan(case):
    desc, emp_waiting, demand = CASES[case]
    snap = datetime.now(timezone.utc).isoformat()
    states = []
    for q, waiting in ((EMP, emp_waiting), (GS, 3)):
        states.append({"instanceId": q[0], "queueId": q[1], "queueName": q[2], "channel": "VOICE",
                       "contactsInQueue": waiting, "oldestContactAgeSeconds": 120 if waiting else 0,
                       "snapshotTime": snap, "seedTag": TAG})
    today = now_local()
    expires = int((datetime.now(timezone.utc) + timedelta(days=2)).timestamp())
    counters = []
    for key, (day, mtd) in demand.items():
        q = EMP if key == "emp" else GS
        for sk, n in (("DAY#" + today.isoformat(), day), ("MONTH#" + today.strftime("%Y-%m"), mtd)):
            counters.append({"PK": "QUEUE#" + q[3], "SK": sk, "contacts": n, "expiresAt": expires, "seedTag": TAG})
    return desc, states, counters


def conv(item):
    return json.loads(json.dumps(item), parse_float=Decimal)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["show", "seed", "clear"])
    p.add_argument("case", nargs="?", choices=sorted(CASES))
    p.add_argument("--counters")
    p.add_argument("--queue-state", default="macp-uk-mgmt-dev-queue-state")
    p.add_argument("--apply", action="store_true")
    a = p.parse_args()
    for n in (a.counters or "", a.queue_state):
        if "prod" in n.lower() or "live" in n.lower():
            sys.exit("Refusing: %s looks like production" % n)
    if a.command != "show" and not a.counters:
        sys.exit("--counters <demand counters table name> is required (Lambda env var DEMAND_COUNTERS_TABLE_NAME)")
    ddb = boto3.resource("dynamodb", region_name="eu-west-2")

    if a.command in ("show", "seed"):
        if not a.case:
            sys.exit("give a case letter A-G")
        desc, states, counters = plan(a.case)
        print("Case %s - %s" % (a.case, desc))
        if a.command == "show":
            print(json.dumps({"queue state": states, "counters": counters}, indent=1, default=str))
            return
        tq, tc = ddb.Table(a.queue_state), ddb.Table(a.counters)
        backups = json.load(open(BACKUP)) if os.path.exists(BACKUP) else []
        for it in states:
            key = {"instanceId": it["instanceId"], "queueId": it["queueId"]}
            old = tq.get_item(Key=key).get("Item")
            if old and old.get("seedTag") != TAG:
                backups.append(json.loads(json.dumps(old, default=lambda o: int(o) if o == int(o) else float(o))))
                print("  existing real row for %s backed up" % it["queueName"])
            print(("WRITE " if a.apply else "would write ") + a.queue_state, it["queueName"], it["contactsInQueue"], "waiting")
            if a.apply:
                tq.put_item(Item=conv(it))
        old_tags = tc.scan(FilterExpression="seedTag = :t", ExpressionAttributeValues={":t": TAG}).get("Items", [])
        for it in old_tags:
            print(("DELETE " if a.apply else "would delete ") + a.counters, it["PK"], it["SK"])
            if a.apply:
                tc.delete_item(Key={"PK": it["PK"], "SK": it["SK"]})
        for it in counters:
            real = tc.get_item(Key={"PK": it["PK"], "SK": it["SK"]}).get("Item")
            if real and real.get("seedTag") != TAG:
                print("  SKIPPED %s %s - a real counter exists" % (it["PK"], it["SK"]))
                continue
            print(("WRITE " if a.apply else "would write ") + a.counters, it["PK"], it["SK"], it["contacts"])
            if a.apply:
                tc.put_item(Item=conv(it))
        if a.apply:
            json.dump(backups, open(BACKUP, "w"))
            print("\nNow run supply_calc within 2 minutes with:")
            print(json.dumps({"email": "t.mind@maximusuk.co.uk", "instanceId": "354ed575-0151-4fc8-ac36-c4393ef35f8b",
                              "dryRun": False}))
        else:
            print("\nDry run only - add --apply to write.")
        return

    tq, tc = ddb.Table(a.queue_state), ddb.Table(a.counters)
    for t, keys in ((tq, ("instanceId", "queueId")), (tc, ("PK", "SK"))):
        for it in t.scan(FilterExpression="seedTag = :t", ExpressionAttributeValues={":t": TAG}).get("Items", []):
            key = {k: it[k] for k in keys}
            print(("DELETE " if a.apply else "would delete ") + t.name, key)
            if a.apply:
                t.delete_item(Key=key)
    if os.path.exists(BACKUP):
        for it in json.load(open(BACKUP)):
            print(("RESTORE " if a.apply else "would restore ") + tq.name, it.get("queueName"))
            if a.apply:
                tq.put_item(Item=conv(it))
        if a.apply:
            os.remove(BACKUP)
    if not a.apply:
        print("Dry run only - add --apply to write.")


if __name__ == "__main__":
    main()
