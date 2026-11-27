import json
import os
import logging
import re
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")

CONFIG_TABLE_NAME = os.environ["QUEUE_PRIORITY_CONFIG_TABLE_NAME"]
SCORES_TABLE_NAME = os.environ["PRIORITY_SCORES_TABLE_NAME"]

config_table = dynamodb.Table(CONFIG_TABLE_NAME)
scores_table = dynamodb.Table(SCORES_TABLE_NAME)

MANDATORY_PRIORITY_THRESHOLD = 40


def queue_key(name):
    """Canonical queue key - must match supply_calc.queue_key. 'GamStop Online General Enquiries_Tel'
    and 'Gamstop_Online_General_Enquiries_Tel' become the same key."""
    return re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower()).strip("_")


class DecimalEncoder(json.JSONEncoder):
    """
    Allows json.dumps() to serialize Decimal values (required by
    DynamoDB for numeric fields) back out as plain floats in the
    JSON response body.
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def lambda_handler(event, context):
    try:
        dry_run_raw = event.get("dryRun", False)
        if isinstance(dry_run_raw, str):
            dry_run = dry_run_raw.strip().lower() == "true"
        else:
            dry_run = bool(dry_run_raw)

        queue_configs = scan_queue_priority_configs()

        results = []
        failures = []

        for config in queue_configs:
            try:
                result = calculate_priority_score(config)
                results.append(result)
            except Exception as exc:
                logger.exception("Failed calculating priority for %s", config.get("queueName"))
                failures.append({"queueName": config.get("queueName"), "error": str(exc)})

        ranked_results = rank_results(results)

        written = 0
        if not dry_run:
            written = batch_write_results(ranked_results)

        return build_response(
            200,
            {
                "message": "Priority calculation completed",
                "processed": len(queue_configs),
                "calculated": len(results),
                "written": written,
                "failed": len(failures),
                "failures": failures,
                "results": ranked_results,
            },
        )

    except (ClientError, BotoCoreError) as exc:
        logger.exception("AWS service error")
        return build_response(500, {"message": "AWS service error", "error": str(exc)})
    except Exception as exc:
        logger.exception("Priority calculation failed")
        return build_response(500, {"message": "Priority calculation failed", "error": str(exc)})


def scan_queue_priority_configs() -> list[dict]:
    items = []
    scan_kwargs = {"FilterExpression": boto3.dynamodb.conditions.Attr("SK").eq("PRIORITY_CONFIG")}

    while True:
        response = config_table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return items


def calculate_priority_score(config: dict) -> dict:
    """
    Confirmed formula, verified against 48 real queues from Daryll's
    CX Rules Matrix (100% match, no discrepancies):

    IF vulnerability >= 40: total = vulnerability (Mandatory Priority,
        bypasses everything else)
    ELSE: total = vulnerability * slaCriticality * clientValueTier * journeyStage
    """
    vulnerability = float(config["vulnerabilityScore"])
    sla_criticality = float(config["slaCriticality"])
    client_value = float(config["clientValueTier"])
    journey_stage = float(config["journeyStage"])

    if vulnerability >= MANDATORY_PRIORITY_THRESHOLD:
        total_score = vulnerability
        mandatory_priority = True
    else:
        total_score = round(vulnerability * sla_criticality * client_value * journey_stage, 2)
        mandatory_priority = False

    daily_cap = config.get("dailyCap")      # Layer 3: contacts per day this queue is expected to receive
    result = {
        "PK": f"QUEUE#{queue_key(config['queueName'])}",
        "SK": "PRIORITY_SCORE",
        "queueName": config["queueName"],
        "instance": config.get("instance"),
        "vulnerabilityScore": Decimal(str(vulnerability)),
        "slaCriticality": Decimal(str(sla_criticality)),
        "clientValueTier": Decimal(str(client_value)),
        "journeyStage": Decimal(str(journey_stage)),
        "totalScore": Decimal(str(total_score)),
        "mandatoryPriority": mandatory_priority,
    }
    if daily_cap is not None and str(daily_cap) != "":
        result["dailyCap"] = Decimal(str(daily_cap))
    return result


def rank_results(results: list[dict]) -> list[dict]:
    sorted_results = sorted(results, key=lambda r: r["totalScore"], reverse=True)
    for i, result in enumerate(sorted_results, start=1):
        result["bureauRank"] = i
    return sorted_results


def batch_write_results(results: list[dict]) -> int:
    with scores_table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as batch:
        for result in results:
            batch.put_item(Item=result)
    return len(results)


def build_response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "statusDescription": f"{status_code} {'OK' if status_code == 200 else 'Error'}",
        "isBase64Encoded": False,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, cls=DecimalEncoder),
    }
