"""
load_priority_config.py - load queue_priority_seed_v2.json into the queue PRIORITY CONFIG table
(the table priority_calc reads: environment variable QUEUE_PRIORITY_CONFIG_TABLE_NAME).

  python3 load_priority_config.py <table-name> queue_priority_seed_v2.json            (preview, writes nothing)
  python3 load_priority_config.py <table-name> queue_priority_seed_v2.json --apply

Each item is written under its canonical key (PK QUEUE#carers_uk_tel, SK PRIORITY_CONFIG), so re-running is safe.
Old items with the previous spelling (PK QUEUE#Carers_UK_Tel) are left alone; remove them later if you want.
Refuses table names containing "prod" or "live".
"""
import json
import sys
from decimal import Decimal

import boto3

args = [a for a in sys.argv[1:] if not a.startswith("--")]
apply = "--apply" in sys.argv
if len(args) != 2:
    sys.exit(__doc__)
table_name, path = args
if "prod" in table_name.lower() or "live" in table_name.lower():
    sys.exit("Refusing: %s looks like production" % table_name)
items = json.load(open(path, encoding="utf-8"))
table = boto3.resource("dynamodb", region_name="eu-west-2").Table(table_name)
for it in items:
    print(("WRITE " if apply else "would write ") + it["PK"], "dailyCap=%s" % it.get("dailyCap", "none"))
    if apply:
        table.put_item(Item=json.loads(json.dumps(it), parse_float=Decimal))
print("%d item(s) %s." % (len(items), "written" if apply else "planned - add --apply to write"))
