"""Central Amazon Connect metrics collector (management-console account).

This Lambda replaces the per-source-account "mgmt-supply" Lambda. It is
deployed once, in the management-console account, and does two independent
jobs depending on how it is invoked:

LIVE path
---------
Triggered by individual Connect-originated events arriving on the ingestion
bus (native Connect events on the default bus, and Amazon Connect Agent
Event Stream records relayed onto the bus from Kinesis). Each invocation:

  1. Resolves the source AWS account + Connect instance from the triggering
     event.
  2. Looks up that instance's configuration row in the instance-details
     table.
  3. Assumes the configured role in the source account.
  4. Calls GetCurrentMetricData + GetCurrentUserData (current, point-in-time
     data) for that one instance.
  5. Publishes INSTANCE_STATUS_UPDATED and InstanceQueueStateCollected
     events back onto the same ingestion bus, with payloads identical to
     the ones the original mgmt-supply Lambda published.
  6. Writes STATUS / QUEUE_STATE rows into the results table.

An EventBridge rule that only matches events whose Source is genuinely
Connect-originated (native Connect sources / the Kinesis-relay source) must
be used to invoke this path, so that the events this Lambda itself publishes
(Source "ac.instance.*") never re-trigger it.

HISTORICAL path
----------------
Triggered on a schedule (an EventBridge Scheduled Rule invoking this Lambda
directly, e.g. every minute -- independent of the ingestion bus). Each
invocation:

  1. Scans the instance-details table for every enabled instance.
  2. Assumes the configured role per instance.
  3. Calls GetMetricDataV2 for a trailing window (STATS_OFFSET_HOURS hours
     ending "now"), reproducing the original mgmt-supply request shape
     exactly (same metric list, QUEUE filter, TOTAL interval, AGENT/QUEUE
     groupings).
  4. Publishes InstanceAgentMetricsCollected and InstanceQueueMetricsCollected
     events onto the ingestion bus, with payloads identical to the ones the
     original mgmt-supply Lambda published.
  5. Writes AGENT_METRIC / QUEUE_METRIC rows into the results table.

Both paths write to the *same* results table (different recordType values)
using the existing instanceKey/metricKey physical key convention, and both
publish onto the *same* ingestion bus that live events are read from.

The deployment package has no third-party dependencies beyond the boto3 and
botocore versions supplied by the AWS Lambda Python runtime.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError


LOG = logging.getLogger()
LOG.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

MANAGEMENT_REGION = (
    os.getenv("AWS_REGION")
    or os.getenv("REGION")
    or "eu-west-2"
)

_BOTO_CONFIG = Config(
    connect_timeout=int(os.getenv("AWS_CONNECT_TIMEOUT_SECONDS", "5")),
    read_timeout=int(os.getenv("AWS_READ_TIMEOUT_SECONDS", "30")),
    max_pool_connections=int(os.getenv("AWS_MAX_POOL_CONNECTIONS", "30")),
    retries={"max_attempts": 4, "mode": "standard"},
)

_STS = boto3.client("sts", region_name=MANAGEMENT_REGION, config=_BOTO_CONFIG)
_EVENTS = boto3.client("events", region_name=MANAGEMENT_REGION, config=_BOTO_CONFIG)

_CACHE_LOCK = threading.Lock()
_ROLE_CLIENT_CACHE: Dict[Tuple[str, str, Optional[str]], Dict[str, Any]] = {}
_QUEUE_CACHE: Dict[str, Dict[str, Any]] = {}
_INSTANCE_CACHE: Dict[str, Dict[str, Any]] = {}
_USER_CACHE: Dict[str, Dict[str, Any]] = {}

# Exact metric list used by the original instance-side Lambda. Do not change
# this list: GetMetricDataV2 can determine returned rows from the request as
# a whole, so even a seemingly harmless metric change would change downstream
# payload shape.
ORIGINAL_HISTORICAL_METRICS = [
    {"Name": "CONTACTS_HANDLED"},
    {"Name": "SUM_HANDLE_TIME"},
    {"Name": "AVG_AFTER_CONTACT_WORK_TIME"},
    {"Name": "AVG_HOLD_TIME"},
    {"Name": "ABANDONMENT_RATE"},
    {"Name": "AGENT_ANSWER_RATE"},
    {"Name": "AGENT_NON_RESPONSE"},
    {"Name": "AGENT_OCCUPANCY"},
    {"Name": "AVG_ABANDON_TIME"},
    {"Name": "MAX_QUEUED_TIME"},
    {"Name": "CONTACTS_ABANDONED"},
]

# Exact metric list used by the original instance-side Lambda's current-
# metrics (live) collection.
RULES_ENGINE_QUEUE_METRICS = [
    {"Name": "CONTACTS_IN_QUEUE", "Unit": "COUNT"},
    {"Name": "OLDEST_CONTACT_AGE", "Unit": "SECONDS"},
    {"Name": "AGENTS_ONLINE", "Unit": "COUNT"},
    {"Name": "AGENTS_AVAILABLE", "Unit": "COUNT"},
    {"Name": "AGENTS_STAFFED", "Unit": "COUNT"},
    {"Name": "AGENTS_ON_CONTACT", "Unit": "COUNT"},
    {"Name": "AGENTS_AFTER_CONTACT_WORK", "Unit": "COUNT"},
]

# Event Source/DetailType pairs. These must stay byte-identical to the
# original mgmt-supply Lambda so downstream event-consuming Lambdas keep
# working unchanged.
EVT_STATUS_SOURCE = "ac.instance.metrics"
EVT_STATUS_DETAIL_TYPE = "INSTANCE_STATUS_UPDATED"
EVT_QUEUE_STATE_SOURCE = "ac.instance.queue-state"
EVT_QUEUE_STATE_DETAIL_TYPE = "InstanceQueueStateCollected"
EVT_AGENT_METRICS_SOURCE = "ac.instance.metricsv2.agents"
EVT_AGENT_METRICS_DETAIL_TYPE = "InstanceAgentMetricsCollected"
EVT_QUEUE_METRICS_SOURCE = "ac.instance.metricsv2.queues"
EVT_QUEUE_METRICS_DETAIL_TYPE = "InstanceQueueMetricsCollected"

# Candidate field names used to locate the source AWS account ID / Connect
# instance reference inside a triggering Connect-originated event. Native
# Connect events on the default bus and Agent Event Stream records relayed
# from Kinesis do not necessarily use the same field names -- verify these
# against a real captured event once deployed and extend the tuples below if
# needed. account_id normally does not need a detail-level fallback because
# every EventBridge event carries the originating account at the envelope
# level ("account"), but the fallbacks are kept for direct/manual testing.
DETAIL_INSTANCE_REF_FIELDS = (
    "instanceArn",
    "InstanceArn",
    "InstanceARN",
    "instanceId",
    "InstanceId",
    "InstanceID",
    "ConnectInstanceId",
    "connectInstanceId",
)
DETAIL_ACCOUNT_ID_FIELDS = (
    "awsAccountId",
    "AccountId",
    "accountId",
    "AwsAccountId",
)

_ROLE_SESSION_SAFE = re.compile(r"[^A-Za-z0-9+=,.@_-]")


@dataclass(frozen=True)
class TargetInstance:
    """Configuration for one cross-account Amazon Connect instance."""

    configured_alias: str
    instance_name: str
    account_id: str
    instance_id: str
    region: str
    role_arn: str
    external_id: Optional[str]
    routing_profile_ids: Tuple[str, ...]
    time_zone: str

    @property
    def instance_key(self) -> str:
        return f"ACCOUNT#{self.account_id}#INSTANCE#{self.instance_id}"

    @property
    def cache_key(self) -> str:
        return f"{self.account_id}:{self.region}:{self.instance_id}"


@dataclass(frozen=True)
class InstanceMetadata:
    instance_alias: str
    instance_arn: str
    instance_status: Optional[str]


@dataclass
class CollectedInstance:
    target: TargetInstance
    collection_id: str
    data_items: List[Dict[str, Any]]
    marker_item: Dict[str, Any]
    response_summary: Dict[str, Any]


class ConfigurationError(ValueError):
    """Raised when an instance-details item is incomplete or invalid."""


class PartialCollectionError(RuntimeError):
    """Raised after all possible targets have run when one or more failed."""


class UnresolvedSourceError(RuntimeError):
    """Raised when a live-trigger event cannot be mapped to a configured instance."""


# ---------------------------------------------------------------------------
# Environment and value helpers
# ---------------------------------------------------------------------------


def env_required(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ConfigurationError(f"Missing required environment variable: {name}")
    return value.strip()


def env_int(name: str, default: int, minimum: Optional[int] = None) -> int:
    raw = os.getenv(name)
    value = int(raw) if raw not in (None, "") else default
    if minimum is not None and value < minimum:
        raise ConfigurationError(f"{name} must be at least {minimum}")
    return value


def bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, Decimal)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    raise ConfigurationError(f"Cannot interpret boolean value: {value!r}")


def first_present(item: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        if name in item and item[name] not in (None, ""):
            return item[name]
    return None


def string_list(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        values = value
    else:
        raise ConfigurationError(f"Expected a list or comma-separated string, got {type(value).__name__}")
    return tuple(str(entry).strip() for entry in values if str(entry).strip())


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for position in range(0, len(items), size):
        yield items[position : position + size]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_seconds(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat()


def elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def get_collection_value(collections: List[dict], metric_name: str):
    for collection in collections:
        metric = collection.get("Metric") or {}
        if metric.get("Name") == metric_name:
            return collection.get("Value")
    return None


def int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def parse_aware_datetime(value: Any, field_name: str) -> datetime:
    """Parse an ISO-8601 datetime and require an explicit UTC offset."""

    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ConfigurationError(
                f"{field_name} must be an ISO-8601 datetime with a UTC offset"
            ) from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ConfigurationError(
            f"{field_name} must include a UTC offset, for example +01:00 or Z"
        )
    return parsed


def ddb_safe(value: Any) -> Any:
    """Convert Python values to types accepted by the DynamoDB resource API."""

    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): ddb_safe(entry) for key, entry in value.items()}
    if isinstance(value, tuple):
        return [ddb_safe(entry) for entry in value]
    if isinstance(value, list):
        return [ddb_safe(entry) for entry in value]
    if isinstance(value, set):
        return {ddb_safe(entry) for entry in value}
    return value


def serialisable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): serialisable(entry) for key, entry in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [serialisable(entry) for entry in value]
    return value


def remaining_time_check(context: Any, required_ms: int = 15_000) -> None:
    if context is None or not hasattr(context, "get_remaining_time_in_millis"):
        return
    remaining = int(context.get_remaining_time_in_millis())
    if remaining < required_ms:
        raise TimeoutError(
            f"Only {remaining} ms remains in the Lambda invocation; refusing to start another target"
        )


# ---------------------------------------------------------------------------
# Historical window (trailing offset, replaces the old midnight-to-now logic)
# ---------------------------------------------------------------------------


def resolve_historical_window(
    event: Mapping[str, Any],
    time_zone: str,
    now: Optional[datetime] = None,
) -> Tuple[datetime, datetime, Dict[str, str]]:
    """Return the UTC API bounds and window metadata for the trailing window.

    Reproduces the original mgmt-supply Lambda's STATS_OFFSET_HOURS logic
    exactly, including the fact that the "start" field is populated from the
    UTC-converted start timestamp rather than a local-time one (the "end"
    field uses the local timestamp). This asymmetry exists in the original
    Lambda's published events and is preserved here deliberately so every
    downstream consumer of these events keeps seeing the same shape.
    """

    tz = ZoneInfo(time_zone)
    now_local = (now.astimezone(tz) if now else datetime.now(tz))

    offset = str(
        first_present(event, ("offsetHours", "offset_hours"))
        or os.getenv("STATS_OFFSET_HOURS", "12")
    )
    offset_int = int(offset)

    now_offset = now_local - timedelta(hours=offset_int)

    start_time = now_offset.astimezone(timezone.utc)
    end_time = now_local.astimezone(timezone.utc)

    # Preserved as-is from the original Lambda: "start" is the UTC instant,
    # "end" is the local-zone instant. Both isoformat() strings carry an
    # explicit offset, so this is unambiguous even though it is asymmetric.
    start_str = start_time.replace(microsecond=0).isoformat()
    end_str = now_local.replace(microsecond=0).isoformat()

    window = {
        "type": "OFFSET: -" + offset + "hrs",
        "timeZone": time_zone,
        "start": start_str,
        "end": end_str,
    }

    return start_time, end_time, window


# ---------------------------------------------------------------------------
# Configuration table
# ---------------------------------------------------------------------------


def scan_all_items(table: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    request: Dict[str, Any] = {}
    while True:
        response = table.scan(**request)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        request["ExclusiveStartKey"] = last_key
    return items


def normalise_account_id(value: Any) -> str:
    if value is None:
        raise ConfigurationError("awsAccountId is required")
    if isinstance(value, Decimal):
        account_id = str(int(value)).zfill(12)
    else:
        account_id = str(value).strip()
    if not re.fullmatch(r"\d{12}", account_id):
        raise ConfigurationError("awsAccountId must be a 12-digit string")
    return account_id


def build_role_arn(account_id: str, role_name: str, partition: str) -> str:
    cleaned = role_name.strip().lstrip("/")
    if not cleaned:
        raise ConfigurationError("TARGET_ROLE_NAME or assumeRoleName is required")
    return f"arn:{partition}:iam::{account_id}:role/{cleaned}"


def parse_target(item: Mapping[str, Any]) -> Optional[TargetInstance]:
    if not bool_value(item.get("enabled"), default=True):
        return None

    alias = str(first_present(item, ("instanceAlias", "alias")) or "").strip()
    if not alias:
        raise ConfigurationError("instanceAlias is required")

    account_id = normalise_account_id(
        first_present(item, ("awsAccountId", "accountId", "targetAwsAccount"))
    )

    instance_id = str(first_present(item, ("instanceId", "connectInstanceId")) or "").strip()
    if not instance_id:
        raise ConfigurationError("instanceId is required")

    region = str(
        first_present(item, ("region", "awsRegion"))
        or os.getenv("DEFAULT_TARGET_REGION")
        or MANAGEMENT_REGION
    ).strip()

    partition = str(item.get("partition") or os.getenv("AWS_PARTITION") or "aws").strip()

    role_arn = str(
        first_present(item, ("assumeRoleArn", "roleArn")) or ""
    ).strip()
    if not role_arn:
        role_name = str(
            first_present(item, ("assumeRoleName", "roleName"))
            or os.getenv("TARGET_ROLE_NAME")
            or ""
        )
        role_arn = build_role_arn(account_id, role_name, partition)

    external_id_value = first_present(item, ("externalId", "assumeRoleExternalId"))
    if external_id_value is None:
        external_id_value = os.getenv("TARGET_ROLE_EXTERNAL_ID")
    external_id = str(external_id_value).strip() if external_id_value else None

    routing_profile_ids = string_list(
        first_present(item, ("routingProfileIds", "routingProfiles"))
    )

    time_zone = str(
        first_present(item, ("timeZone", "timezone"))
        or os.getenv("TIME_ZONE")
        or "Europe/London"
    ).strip()
    # Validate early so a bad table item is reported as configuration, not midway through collection.
    ZoneInfo(time_zone)

    instance_name = str(item.get("instanceName") or alias).strip()

    return TargetInstance(
        configured_alias=alias,
        instance_name=instance_name,
        account_id=account_id,
        instance_id=instance_id,
        region=region,
        role_arn=role_arn,
        external_id=external_id,
        routing_profile_ids=routing_profile_ids,
        time_zone=time_zone,
    )


def requested_aliases(event: Mapping[str, Any]) -> Set[str]:
    value = first_present(
        event,
        ("instanceAliases", "instance_aliases", "instanceAlias", "instance_alias"),
    )
    if value is None:
        return set()
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        values = value
    else:
        raise ConfigurationError("instanceAliases must be a string or a list of strings")
    return {str(entry).strip() for entry in values if str(entry).strip()}


def load_targets(
    details_table: Any,
    event: Mapping[str, Any],
) -> Tuple[List[TargetInstance], List[Dict[str, str]]]:
    alias_filter = requested_aliases(event)
    targets: List[TargetInstance] = []
    errors: List[Dict[str, str]] = []

    for item in scan_all_items(details_table):
        alias = str(item.get("instanceAlias") or "<unknown>")
        if alias_filter and alias not in alias_filter:
            continue
        try:
            target = parse_target(item)
            if target is not None:
                targets.append(target)
        except Exception as exc:  # configuration errors must not prevent valid rows running
            LOG.exception("Invalid instance-details item for %s", alias)
            errors.append({"instanceAlias": alias, "error": str(exc)})

    if alias_filter:
        found = {target.configured_alias for target in targets}
        invalid = {entry["instanceAlias"] for entry in errors}
        for missing in sorted(alias_filter - found - invalid):
            errors.append(
                {
                    "instanceAlias": missing,
                    "error": "No enabled instance-details item was found",
                }
            )

    targets.sort(key=lambda target: target.configured_alias.lower())
    return targets, errors


def find_target_by_identity(
    details_table: Any,
    account_id: str,
    instance_id: str,
) -> Optional[TargetInstance]:
    """Locate the configuration row for a specific account+instance.

    This scans the whole table, same as load_targets. For a handful of
    configured instances this is cheap; if the number of onboarded instances
    grows large, add a GSI on (awsAccountId, instanceId) and query it here
    instead.
    """

    for item in scan_all_items(details_table):
        try:
            target = parse_target(item)
        except Exception:
            continue
        if target is None:
            continue
        if target.account_id == account_id and target.instance_id == instance_id:
            return target
    return None


# ---------------------------------------------------------------------------
# Cross-account client and warm-invocation caches
# ---------------------------------------------------------------------------


def role_session_name(alias: str) -> str:
    cleaned = _ROLE_SESSION_SAFE.sub("-", alias)
    return f"mgmt-supply-{cleaned}"[:64]


def get_connect_client(target: TargetInstance) -> Any:
    cache_key = (target.role_arn, target.region, target.external_id)
    now = time.time()

    with _CACHE_LOCK:
        cached = _ROLE_CLIENT_CACHE.get(cache_key)
        if cached and cached["refresh_after"] > now:
            return cached["client"]

    request: Dict[str, Any] = {
        "RoleArn": target.role_arn,
        "RoleSessionName": role_session_name(target.configured_alias),
    }
    if target.external_id:
        request["ExternalId"] = target.external_id

    duration_raw = os.getenv("ASSUME_ROLE_DURATION_SECONDS")
    if duration_raw:
        request["DurationSeconds"] = int(duration_raw)

    LOG.info(
        "Assuming role %s for instance %s",
        target.role_arn,
        target.configured_alias,
    )
    response = _STS.assume_role(**request)
    credentials = response["Credentials"]
    expiration = credentials["Expiration"]
    refresh_after = expiration.timestamp() - env_int(
        "ASSUME_ROLE_REFRESH_MARGIN_SECONDS", 300, minimum=0
    )

    client = boto3.client(
        "connect",
        region_name=target.region,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        config=_BOTO_CONFIG,
    )

    with _CACHE_LOCK:
        _ROLE_CLIENT_CACHE[cache_key] = {
            "client": client,
            "refresh_after": refresh_after,
        }
    return client


def get_instance_metadata(connect: Any, target: TargetInstance) -> InstanceMetadata:
    ttl = env_int("INSTANCE_CACHE_TTL_S", 900, minimum=0)
    now = time.time()

    with _CACHE_LOCK:
        cached = _INSTANCE_CACHE.get(target.cache_key)
        if cached and cached["expires_at"] > now:
            return cached["metadata"]

    response = connect.describe_instance(InstanceId=target.instance_id)
    instance = response.get("Instance") or {}
    alias = str(instance.get("InstanceAlias") or target.configured_alias)
    arn = str(instance.get("Arn") or "")
    if not arn:
        partition = os.getenv("AWS_PARTITION", "aws")
        arn = (
            f"arn:{partition}:connect:{target.region}:{target.account_id}:"
            f"instance/{target.instance_id}"
        )

    metadata = InstanceMetadata(
        instance_alias=alias,
        instance_arn=arn,
        instance_status=instance.get("InstanceStatus"),
    )

    if alias != target.configured_alias:
        LOG.warning(
            "Configured alias %s differs from DescribeInstance alias %s",
            target.configured_alias,
            alias,
        )

    with _CACHE_LOCK:
        _INSTANCE_CACHE[target.cache_key] = {
            "metadata": metadata,
            "expires_at": now + ttl,
        }
    return metadata


def list_standard_queues_cached(
    connect: Any,
    target: TargetInstance,
) -> Tuple[List[str], Dict[str, str]]:
    ttl = env_int("QUEUE_CACHE_TTL_S", 900, minimum=0)
    now = time.time()

    with _CACHE_LOCK:
        cached = _QUEUE_CACHE.get(target.cache_key)
        if cached and cached["expires_at"] > now:
            return list(cached["queue_ids"]), dict(cached["queue_map"])

    paginator = connect.get_paginator("list_queues")
    queue_ids: List[str] = []
    queue_map: Dict[str, str] = {}

    for page in paginator.paginate(
        InstanceId=target.instance_id,
        QueueTypes=["STANDARD"],
        PaginationConfig={"PageSize": 100},
    ):
        for queue in page.get("QueueSummaryList", []):
            queue_id = queue.get("Id")
            if not queue_id:
                continue
            queue_ids.append(queue_id)
            queue_map[queue_id] = queue.get("Name") or ""

    deduplicated = list(dict.fromkeys(queue_ids))
    with _CACHE_LOCK:
        _QUEUE_CACHE[target.cache_key] = {
            "queue_ids": deduplicated,
            "queue_map": queue_map,
            "expires_at": now + ttl,
        }
    return deduplicated, queue_map


def describe_user_safe(connect: Any, instance_id: str, user_id: str) -> Tuple[str, Dict[str, Any]]:
    try:
        response = connect.describe_user(InstanceId=instance_id, UserId=user_id)
    except connect.exceptions.ResourceNotFoundException:
        return user_id, {}

    identity = (response.get("User") or {}).get("IdentityInfo") or {}
    first_name = identity.get("FirstName")
    last_name = identity.get("LastName")
    full_name = " ".join(value for value in (first_name, last_name) if value) or None
    return user_id, {
        "firstName": first_name,
        "lastName": last_name,
        "fullName": full_name,
        "email": identity.get("Email"),
    }


def get_user_map(
    connect: Any,
    target: TargetInstance,
    agent_ids: Set[str],
) -> Dict[str, Dict[str, Any]]:
    if not agent_ids:
        return {}

    ttl = env_int("USER_CACHE_TTL_S", 3600, minimum=0)
    now = time.time()

    with _CACHE_LOCK:
        cached = _USER_CACHE.get(target.cache_key)
        if cached and cached["expires_at"] > now:
            user_map = dict(cached["user_map"])
        else:
            user_map = {}

    missing = sorted(agent_ids - set(user_map))
    if missing:
        workers = min(env_int("USER_LOOKUP_WORKERS", 4, minimum=1), len(missing))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    describe_user_safe,
                    connect,
                    target.instance_id,
                    user_id,
                )
                for user_id in missing
            ]
            for future in as_completed(futures):
                user_id, user_info = future.result()
                user_map[user_id] = user_info

        with _CACHE_LOCK:
            _USER_CACHE[target.cache_key] = {
                "user_map": user_map,
                "expires_at": now + ttl,
            }

    return {user_id: user_map.get(user_id, {}) for user_id in agent_ids}


# ===========================================================================
# LIVE (current) metrics -- ported from the original instance-side Lambda
# ===========================================================================


def _get_current_metric_data(
    connect: Any,
    instance_id: str,
    queue_batch: List[str],
    metrics: List[dict],
    next_token: Optional[str] = None,
) -> dict:
    request = {
        "InstanceId": instance_id,
        "Filters": {"Queues": queue_batch},
        "CurrentMetrics": metrics,
        "Groupings": ["QUEUE"],
        "MaxResults": 100,
    }
    if next_token:
        request["NextToken"] = next_token
    return connect.get_current_metric_data(**request)


def empty_queue_state(queue_id: str, queue_name: Optional[str]) -> dict:
    """
    Creates a queue-state record when Connect returns no current metric row.

    Null values are preserved because missing telemetry is not necessarily
    the same as a genuine zero.
    """

    return {
        "queueId": queue_id,
        "queueName": queue_name,
        "channel": "VOICE",
        "contactsInQueue": None,
        "oldestContactAgeSeconds": None,
        "agentsOnline": None,
        "agentsAvailable": None,
        "agentsStaffed": None,
        "agentsOnContact": None,
        "agentsAfterContactWork": None,
    }


def get_current_queue_states(
    connect: Any,
    instance_id: str,
    queue_ids: List[str],
    queue_map: Dict[str, str],
) -> Tuple[List[dict], Optional[str], int, int]:
    """
    Returns queue_states, snapshot_time, total_contacts_in_queue,
    oldest_contact_age_seconds -- identical shape to the original Lambda.
    """

    if not queue_ids:
        return [], None, 0, 0

    queue_states: Dict[str, dict] = {}
    latest_snapshot_time: Optional[datetime] = None

    for queue_batch in chunks(queue_ids, 50):
        next_token: Optional[str] = None

        while True:
            response = _get_current_metric_data(
                connect=connect,
                instance_id=instance_id,
                queue_batch=list(queue_batch),
                metrics=RULES_ENGINE_QUEUE_METRICS,
                next_token=next_token,
            )

            snapshot_time = response.get("DataSnapshotTime")
            if snapshot_time and (
                latest_snapshot_time is None or snapshot_time > latest_snapshot_time
            ):
                latest_snapshot_time = snapshot_time

            for result in response.get("MetricResults", []):
                dimensions = result.get("Dimensions") or {}
                queue_dimension = dimensions.get("Queue") or {}
                queue_id = queue_dimension.get("Id")
                if not queue_id:
                    continue

                collections = result.get("Collections") or []
                queue_name = queue_map.get(queue_id) or ""
                lowered = queue_name.lower()

                if lowered.endswith("_tel"):
                    queue_channel = "VOICE"
                elif lowered.endswith("_vm") or "voicemail" in lowered:
                    queue_channel = "VOICEMAIL"
                else:
                    queue_channel = "CHAT"

                queue_states[queue_id] = {
                    "queueId": queue_id,
                    "queueName": queue_map.get(queue_id),
                    "channel": queue_channel,
                    "contactsInQueue": int_or_none(
                        get_collection_value(collections, "CONTACTS_IN_QUEUE")
                    ),
                    "oldestContactAgeSeconds": int_or_none(
                        get_collection_value(collections, "OLDEST_CONTACT_AGE")
                    ),
                    "agentsOnline": int_or_none(
                        get_collection_value(collections, "AGENTS_ONLINE")
                    ),
                    "agentsAvailable": int_or_none(
                        get_collection_value(collections, "AGENTS_AVAILABLE")
                    ),
                    "agentsStaffed": int_or_none(
                        get_collection_value(collections, "AGENTS_STAFFED")
                    ),
                    "agentsOnContact": int_or_none(
                        get_collection_value(collections, "AGENTS_ON_CONTACT")
                    ),
                    "agentsAfterContactWork": int_or_none(
                        get_collection_value(collections, "AGENTS_AFTER_CONTACT_WORK")
                    ),
                }

            next_token = response.get("NextToken")
            if not next_token:
                break

    # Connect may omit a queue when no applicable current metric data exists.
    # Include it with null metric values so the controller knows the queue is
    # still part of the instance.
    for queue_id in queue_ids:
        if queue_id not in queue_states:
            queue_states[queue_id] = empty_queue_state(
                queue_id=queue_id,
                queue_name=queue_map.get(queue_id),
            )

    ordered_states = sorted(
        queue_states.values(),
        key=lambda item: (
            (item.get("queueName") or "").lower(),
            item.get("queueId") or "",
        ),
    )

    total_contacts_in_queue = 0
    oldest_contact_age_seconds = 0
    for state in ordered_states:
        contacts = state.get("contactsInQueue")
        oldest = state.get("oldestContactAgeSeconds")
        if contacts is not None:
            total_contacts_in_queue += contacts
        if oldest is not None:
            oldest_contact_age_seconds = max(oldest_contact_age_seconds, oldest)

    snapshot_time_text = (
        latest_snapshot_time.isoformat() if latest_snapshot_time else None
    )

    return (
        ordered_states,
        snapshot_time_text,
        int(total_contacts_in_queue),
        int(oldest_contact_age_seconds),
    )


def _page_user_data(connect: Any, instance_id: str, filters: dict) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    next_token: Optional[str] = None

    while True:
        request: dict = {
            "InstanceId": instance_id,
            "MaxResults": 100,
            "Filters": filters,
        }
        if next_token:
            request["NextToken"] = next_token

        response = connect.get_current_user_data(**request)

        for user in response.get("UserDataList", []):
            status = (user.get("Status") or {}).get("StatusName") or "Unknown"
            counts[status] = counts.get(status, 0) + 1

        next_token = response.get("NextToken")
        if not next_token:
            break

    return counts


def get_agent_state_counts(
    connect: Any,
    instance_id: str,
    queue_ids: List[str],
    routing_profile_ids: Optional[List[str]],
) -> Dict[str, int]:
    """
    Returns status-name to agent-count values.

    Queue scoping is attempted first; routing-profile filters are used as a
    fallback, matching the original Lambda's behaviour.
    """

    if queue_ids:
        try:
            return _page_user_data(connect, instance_id, {"Queues": queue_ids})
        except ParamValidationError:
            pass

    if routing_profile_ids:
        for filter_key in ("RoutingProfiles", "RoutingProfileIds"):
            try:
                return _page_user_data(
                    connect,
                    instance_id,
                    {filter_key: routing_profile_ids},
                )
            except ParamValidationError:
                continue

    raise ConfigurationError(
        "GetCurrentUserData requires scoping. Provide routingProfileIds on "
        "the instance-details row if queue scoping is not supported."
    )


def collect_live_status(
    connect: Any,
    target: TargetInstance,
    queue_ids: List[str],
    queue_map: Dict[str, str],
) -> Dict[str, Any]:
    """Current (point-in-time) queue depths and agent states for one instance."""

    started = time.perf_counter()

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_queue_states = executor.submit(
            get_current_queue_states,
            connect,
            target.instance_id,
            queue_ids,
            queue_map,
        )
        future_agents = executor.submit(
            get_agent_state_counts,
            connect,
            target.instance_id,
            queue_ids,
            list(target.routing_profile_ids) or None,
        )

        queue_states, queue_snapshot_time, total_in_queue, oldest_age_s = (
            future_queue_states.result()
        )
        agent_states = future_agents.result()

    return {
        "queueCount": len(queue_ids),
        "totalContactsInQueue": total_in_queue,
        "oldestContactAgeSeconds": oldest_age_s,
        "agentStates": agent_states,
        "queueStates": queue_states,
        "queueSnapshotTime": queue_snapshot_time,
        "msTotal": elapsed_ms(started),
    }


# ===========================================================================
# HISTORICAL (trailing-window) metrics
# ===========================================================================


def get_metrics_batch(
    connect: Any,
    instance_arn: str,
    queue_ids: Sequence[str],
    start_time: datetime,
    end_time: datetime,
    time_zone: str,
    grouping: str,
    max_results: int,
) -> List[Dict[str, Any]]:
    """Run the same GetMetricDataV2 request as the original Lambda."""

    request: Dict[str, Any] = {
        "ResourceArn": instance_arn,
        "StartTime": start_time,
        "EndTime": end_time,
        "Interval": {"TimeZone": time_zone, "IntervalPeriod": "TOTAL"},
        "Filters": [{"FilterKey": "QUEUE", "FilterValues": list(queue_ids)}],
        "Groupings": [grouping],
        "Metrics": list(ORIGINAL_HISTORICAL_METRICS),
        "MaxResults": max_results,
    }

    rows: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    while True:
        if next_token:
            request["NextToken"] = next_token
        else:
            request.pop("NextToken", None)

        response = connect.get_metric_data_v2(**request)
        for result in response.get("MetricResults", []):
            dimensions = result.get("Dimensions") or {}
            interval = result.get("MetricInterval") or {}
            row: Dict[str, Any] = {
                "agentId": dimensions.get("AGENT"),
                "queueId": dimensions.get("QUEUE"),
                "intervalStart": (
                    interval["StartTime"].isoformat() if interval.get("StartTime") else None
                ),
                "intervalEnd": (
                    interval["EndTime"].isoformat() if interval.get("EndTime") else None
                ),
            }
            for collection in result.get("Collections", []):
                metric_name = (collection.get("Metric") or {}).get("Name")
                if metric_name:
                    row[metric_name] = collection.get("Value")
            rows.append(row)

        next_token = response.get("NextToken")
        if not next_token:
            break

    return rows


def merge_agent_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Preserve the original Lambda's one-row-per-agent aggregation behaviour."""

    merged: Dict[str, Dict[str, Any]] = {}
    non_additive_metrics = (
        "AVG_AFTER_CONTACT_WORK_TIME",
        "AVG_HOLD_TIME",
        "ABANDONMENT_RATE",
        "AGENT_ANSWER_RATE",
        "AGENT_NON_RESPONSE",
        "AGENT_OCCUPANCY",
        "AVG_ABANDON_TIME",
        "MAX_QUEUED_TIME",
        "CONTACTS_ABANDONED",
    )

    for row in rows:
        agent_id = row.get("agentId")
        if not agent_id:
            continue
        if agent_id not in merged:
            merged[agent_id] = {
                "agentId": agent_id,
                "CONTACTS_HANDLED": 0,
                "SUM_HANDLE_TIME": 0,
                **{metric: row.get(metric) for metric in non_additive_metrics},
                "intervalStart": row.get("intervalStart"),
                "intervalEnd": row.get("intervalEnd"),
            }

        merged_row = merged[agent_id]
        merged_row["CONTACTS_HANDLED"] += row.get("CONTACTS_HANDLED", 0) or 0
        merged_row["SUM_HANDLE_TIME"] += row.get("SUM_HANDLE_TIME", 0) or 0
        for metric in non_additive_metrics:
            if row.get(metric) is not None:
                merged_row[metric] = row[metric]
        if row.get("intervalStart"):
            merged_row["intervalStart"] = row["intervalStart"]
        if row.get("intervalEnd"):
            merged_row["intervalEnd"] = row["intervalEnd"]

    return list(merged.values())


def normalise_metric_row(
    row: Mapping[str, Any],
    entity_type: str,
    queue_map: Mapping[str, str],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "intervalStart": row.get("intervalStart"),
        "intervalEnd": row.get("intervalEnd"),
        "contactsHandled": row.get("CONTACTS_HANDLED", 0),
        "sumHandleTimeSeconds": row.get("SUM_HANDLE_TIME", 0),
        "avgAfterContactWorkTimeSeconds": row.get("AVG_AFTER_CONTACT_WORK_TIME"),
        "avgHoldTimeSeconds": row.get("AVG_HOLD_TIME"),
        "abandonmentRate": row.get("ABANDONMENT_RATE"),
        "agentAnswerRate": row.get("AGENT_ANSWER_RATE"),
        "agentNonResponse": row.get("AGENT_NON_RESPONSE"),
        "agentOccupancy": row.get("AGENT_OCCUPANCY"),
        "avgAbandonTimeSeconds": row.get("AVG_ABANDON_TIME"),
        "maxQueuedTimeSeconds": row.get("MAX_QUEUED_TIME"),
        "contactsAbandoned": row.get("CONTACTS_ABANDONED"),
    }
    if entity_type == "AGENT":
        result["agentId"] = row.get("agentId")
    else:
        queue_id = row.get("queueId")
        result["queueId"] = queue_id
        result["queueName"] = queue_map.get(str(queue_id)) if queue_id else None
    return result


def collect_historical_metrics(
    connect: Any,
    target: TargetInstance,
    metadata: InstanceMetadata,
    queue_ids: Sequence[str],
    queue_map: Mapping[str, str],
    event: Mapping[str, Any],
) -> Dict[str, Any]:
    time_zone = str(
        first_present(event, ("timeZone", "time_zone")) or target.time_zone
    )
    max_results = int(
        first_present(event, ("maxResults", "max_results"))
        or os.getenv("MAX_RESULTS")
        or "100"
    )
    if not 1 <= max_results <= 100:
        raise ConfigurationError("maxResults must be between 1 and 100")

    queue_lookup_started = time.perf_counter_ns()
    # queue_ids/queue_map are already resolved by the caller; this timing
    # exists purely so the published event's timingsUs shape matches the
    # original Lambda's structure.
    queue_lookup_finished = time.perf_counter_ns()

    start_time, end_time, window = resolve_historical_window(
        event=event,
        time_zone=time_zone,
    )
    tz = ZoneInfo(time_zone)
    collected_at_local = iso_seconds(datetime.now(tz))

    LOG.info(
        "Historical metrics window instance=%s start=%s end=%s utc_start=%s utc_end=%s queues=%s",
        target.configured_alias,
        window["start"],
        window["end"],
        iso_seconds(start_time),
        iso_seconds(end_time),
        len(queue_ids),
    )

    if not queue_ids:
        return {
            "window": window,
            "collectedAt": collected_at_local,
            "agentMetrics": [],
            "queueMetrics": [],
            "rawAgentRowCount": 0,
            "rawQueueRowCount": 0,
            "timingsUs": {
                "queueLookup": queue_lookup_finished - queue_lookup_started,
                "metricsFetch": 0,
                "agentMerge": 0,
                "userLookup": 0,
            },
        }

    metrics_started = time.perf_counter_ns()
    agent_rows: List[Dict[str, Any]] = []
    queue_rows: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        for queue_batch in chunks(queue_ids, 100):
            agent_future = executor.submit(
                get_metrics_batch,
                connect,
                metadata.instance_arn,
                queue_batch,
                start_time,
                end_time,
                time_zone,
                "AGENT",
                max_results,
            )
            queue_future = executor.submit(
                get_metrics_batch,
                connect,
                metadata.instance_arn,
                queue_batch,
                start_time,
                end_time,
                time_zone,
                "QUEUE",
                max_results,
            )
            agent_rows.extend(agent_future.result())
            queue_rows.extend(queue_future.result())

    metrics_finished = time.perf_counter_ns()

    merge_started = time.perf_counter_ns()
    merged_agent_rows = merge_agent_rows(agent_rows)
    merge_finished = time.perf_counter_ns()

    agent_ids = {
        str(row["agentId"])
        for row in merged_agent_rows
        if row.get("agentId")
    }

    users_started = time.perf_counter_ns()
    user_map = get_user_map(connect, target, agent_ids)
    users_finished = time.perf_counter_ns()

    agent_metrics: List[Dict[str, Any]] = []
    for row in merged_agent_rows:
        metric = normalise_metric_row(row, "AGENT", queue_map)
        user_info = user_map.get(str(row.get("agentId")), {})
        metric.update(
            {
                "firstName": user_info.get("firstName"),
                "lastName": user_info.get("lastName"),
                "fullName": user_info.get("fullName"),
                "email": user_info.get("email"),
            }
        )
        agent_metrics.append(metric)

    queue_metrics = [
        normalise_metric_row(row, "QUEUE", queue_map)
        for row in queue_rows
    ]

    LOG.info(
        "Historical metrics returned instance=%s raw_agent_rows=%s merged_agents=%s queue_rows=%s",
        target.configured_alias,
        len(agent_rows),
        len(agent_metrics),
        len(queue_metrics),
    )

    return {
        "window": window,
        "collectedAt": collected_at_local,
        "agentMetrics": agent_metrics,
        "queueMetrics": queue_metrics,
        "rawAgentRowCount": len(agent_rows),
        "rawQueueRowCount": len(queue_rows),
        "metricRequestProfile": "ORIGINAL_INSTANCE_LAMBDA",
        "requestedMetricNames": [
            metric["Name"] for metric in ORIGINAL_HISTORICAL_METRICS
        ],
        "timingsUs": {
            "queueLookup": queue_lookup_finished - queue_lookup_started,
            "metricsFetch": metrics_finished - metrics_started,
            "agentMerge": merge_finished - merge_started,
            "userLookup": users_finished - users_started,
        },
    }


# ===========================================================================
# Event publishing (payloads identical to the original mgmt-supply Lambda)
# ===========================================================================


def _sized_json(detail: dict) -> str:
    """Serialises the payload, records its size and serialises again."""

    raw = json.dumps(detail, separators=(",", ":"), default=str)
    detail["payloadSizeBytes"] = len(raw.encode("utf-8"))
    return json.dumps(detail, separators=(",", ":"), default=str)


def publish_status_event(event_bus_arn: str, detail: dict) -> None:
    """Publishes the INSTANCE_STATUS_UPDATED event. Payload shape unchanged."""

    response = _EVENTS.put_events(
        Entries=[
            {
                "Source": EVT_STATUS_SOURCE,
                "DetailType": EVT_STATUS_DETAIL_TYPE,
                "EventBusName": event_bus_arn,
                "Detail": json.dumps(detail, default=str),
                "Resources": [detail["instanceId"]],
            }
        ]
    )
    LOG.info("publish_status_event response: %s", response)
    if response.get("FailedEntryCount", 0):
        raise RuntimeError(f"Failed to publish status event: {response}")


def publish_queue_state_event(
    event_bus_arn: str,
    instance_id: str,
    instance_alias: Optional[str],
    snapshot_time: Optional[str],
    queue_states: List[dict],
) -> None:
    """Publishes InstanceQueueStateCollected. Payload shape unchanged."""

    detail = {
        "instanceId": instance_id,
        "instanceAlias": instance_alias,
        "snapshotTime": snapshot_time,
        "collectedAt": (
            datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        ),
        "queueCount": len(queue_states),
        "queues": queue_states,
        "schemaVersion": 1,
    }

    response = _EVENTS.put_events(
        Entries=[
            {
                "Source": EVT_QUEUE_STATE_SOURCE,
                "DetailType": EVT_QUEUE_STATE_DETAIL_TYPE,
                "EventBusName": event_bus_arn,
                "Detail": json.dumps(detail, separators=(",", ":"), default=str),
                "Resources": [instance_id],
            }
        ]
    )
    LOG.info("publish_queue_state_event response: %s", response)
    if response.get("FailedEntryCount", 0):
        raise RuntimeError(f"Failed to publish queue-state event: {response}")


def build_metrics_event_payloads(
    target: TargetInstance,
    metadata: InstanceMetadata,
    metrics_result: Mapping[str, Any],
    queue_lookup_us: int,
    instance_lookup_us: int,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Builds the agent/queue MetricsV2 event payloads, matching the
    original Lambda's "common" + entityType/count/*Metrics envelope."""

    timings = dict(metrics_result.get("timingsUs") or {})
    timings.setdefault("queueLookup", queue_lookup_us)
    timings["instanceLookup"] = instance_lookup_us
    timings["eventPublish"] = 0
    timings["total"] = sum(
        value for key, value in timings.items() if key != "total"
    )

    common = {
        "instanceId": target.instance_id,
        "instanceAlias": metadata.instance_alias,
        "instanceArn": metadata.instance_arn,
        "window": metrics_result["window"],
        "collectedAt": metrics_result["collectedAt"],
        "timingsUs": timings,
    }

    agent_detail = {
        **common,
        "entityType": "AGENT",
        "count": len(metrics_result["agentMetrics"]),
        "agentMetrics": metrics_result["agentMetrics"],
    }
    queue_detail = {
        **common,
        "entityType": "QUEUE",
        "count": len(metrics_result["queueMetrics"]),
        "queueMetrics": metrics_result["queueMetrics"],
    }
    return agent_detail, queue_detail


def publish_metrics_events(
    event_bus_arn: str,
    agent_detail: dict,
    queue_detail: dict,
) -> dict:
    """Publishes the two MetricsV2 events. Payload shape unchanged."""

    response = _EVENTS.put_events(
        Entries=[
            {
                "Source": EVT_AGENT_METRICS_SOURCE,
                "DetailType": EVT_AGENT_METRICS_DETAIL_TYPE,
                "EventBusName": event_bus_arn,
                "Detail": _sized_json(agent_detail),
            },
            {
                "Source": EVT_QUEUE_METRICS_SOURCE,
                "DetailType": EVT_QUEUE_METRICS_DETAIL_TYPE,
                "EventBusName": event_bus_arn,
                "Detail": _sized_json(queue_detail),
            },
        ]
    )
    if response.get("FailedEntryCount", 0):
        raise RuntimeError(f"Failed to publish metrics events: {response}")
    return response


# ===========================================================================
# DynamoDB result item construction and writes
# ===========================================================================


def add_index_fields(item: Dict[str, Any], record_type: str, gsi_suffix: str) -> None:
    item["recordType"] = record_type
    item["gsiSortKey"] = f"{item['instanceAlias']}#{gsi_suffix}"


def common_result_item(
    target: TargetInstance,
    metadata: InstanceMetadata,
    collection_id: str,
    collected_at: str,
    expires_at: int,
    collection_kind: str,
    metric_source: str,
) -> Dict[str, Any]:
    return {
        "instanceKey": target.instance_key,
        "awsAccountId": target.account_id,
        "instanceId": target.instance_id,
        "instanceArn": metadata.instance_arn,
        "instanceAlias": metadata.instance_alias,
        "configuredInstanceAlias": target.configured_alias,
        "instanceName": target.instance_name,
        "instanceStatus": metadata.instance_status,
        "region": target.region,
        "collectionId": collection_id,
        "collectedAt": collected_at,
        "expiresAt": expires_at,
        "collectionKind": collection_kind,
        "metricSource": metric_source,
        "schemaVersion": 4,
    }


def validate_historical_items(items: Sequence[Mapping[str, Any]]) -> None:
    """Reject any non-historical row before it reaches the results table."""

    required_window_fields = (
        "window",
        "windowType",
        "timeZone",
        "windowStart",
        "windowEnd",
    )
    allowed_types = {"AGENT_METRIC", "QUEUE_METRIC"}

    for item in items:
        record_type = str(item.get("recordType") or "")
        if record_type not in allowed_types:
            raise RuntimeError(
                f"Refusing to write non-historical record type {record_type!r}"
            )
        missing = [field for field in required_window_fields if not item.get(field)]
        if missing:
            raise RuntimeError(
                f"Historical {record_type} item is missing window fields: "
                f"{', '.join(missing)}"
            )


def build_historical_result_items(
    target: TargetInstance,
    metadata: InstanceMetadata,
    collection_id: str,
    metrics_result: Mapping[str, Any],
    collection_started: datetime,
    collection_finished: datetime,
) -> CollectedInstance:
    ttl_seconds = env_int(
        "RESULT_TTL_SECONDS",
        env_int("TTL_SECONDS", 300, minimum=1),
        minimum=1,
    )
    expires_at = int(collection_finished.timestamp()) + ttl_seconds
    collected_at = iso_seconds(collection_finished)
    base = common_result_item(
        target,
        metadata,
        collection_id,
        collected_at,
        expires_at,
        collection_kind="HISTORICAL",
        metric_source="GetMetricDataV2",
    )
    items: List[Dict[str, Any]] = []

    agent_metric_count = 0
    queue_metric_count = 0

    window = dict(metrics_result["window"])
    window_fields = {
        "windowType": window["type"],
        "timeZone": window["timeZone"],
        "windowStart": window["start"],
        "windowEnd": window["end"],
        "intervalPeriod": "TOTAL",
        "metricRequestProfile": str(
            metrics_result.get("metricRequestProfile") or "ORIGINAL_INSTANCE_LAMBDA"
        ),
    }
    metrics_collected_at = str(metrics_result.get("collectedAt") or collected_at)

    for agent_metric in metrics_result.get("agentMetrics", []):
        agent_id = str(agent_metric["agentId"])
        display = agent_metric.get("fullName") or agent_metric.get("email") or agent_id
        item = {
            **base,
            **dict(agent_metric),
            **window_fields,
            "metricKey": f"AGENT_METRIC#{agent_id}",
            "window": window,
            "collectedAt": metrics_collected_at,
            "collectedAtUtc": collected_at,
            "pk": "AGENT",
            "sk": f"{target.instance_id}#{agent_id}",
            "entityType": "AGENT",
            "entityId": agent_id,
        }
        add_index_fields(item, "AGENT_METRIC", f"{display}#{agent_id}")
        items.append(item)
        agent_metric_count += 1

    for queue_metric in metrics_result.get("queueMetrics", []):
        queue_id_value = queue_metric.get("queueId")
        if not queue_id_value:
            LOG.warning(
                "Skipping QUEUE MetricResults row without QUEUE dimension for %s",
                target.configured_alias,
            )
            continue
        queue_id = str(queue_id_value)
        queue_name = str(queue_metric.get("queueName") or queue_id)
        item = {
            **base,
            **dict(queue_metric),
            **window_fields,
            "metricKey": f"QUEUE_METRIC#{queue_id}",
            "window": window,
            "collectedAt": metrics_collected_at,
            "collectedAtUtc": collected_at,
            "pk": "QUEUE",
            "sk": f"{target.instance_id}#{queue_id}",
            "entityType": "QUEUE",
            "entityId": queue_id,
        }
        add_index_fields(item, "QUEUE_METRIC", f"{queue_name}#{queue_id}")
        items.append(item)
        queue_metric_count += 1

    duration_ms = int((collection_finished - collection_started).total_seconds() * 1000)
    marker = {
        **base,
        "metricKey": "COLLECTION",
        "mode": "METRICS",
        "startedAt": iso_seconds(collection_started),
        "completedAt": collected_at,
        "durationMs": duration_ms,
        "dataItemCount": len(items),
        "agentMetricCount": agent_metric_count,
        "queueMetricCount": queue_metric_count,
        "metricsIncluded": True,
        "windowContract": "TRAILING_OFFSET_HOURS",
        "windowType": window["type"],
        "timeZone": window["timeZone"],
        "windowStart": window["start"],
        "windowEnd": window["end"],
        "window": window,
        "intervalPeriod": "TOTAL",
        "rawAgentRowCount": int(metrics_result.get("rawAgentRowCount", 0)),
        "rawQueueRowCount": int(metrics_result.get("rawQueueRowCount", 0)),
        "metricRequestProfile": str(
            metrics_result.get("metricRequestProfile") or "ORIGINAL_INSTANCE_LAMBDA"
        ),
        "requestedMetricNames": list(
            metrics_result.get("requestedMetricNames")
            or [metric["Name"] for metric in ORIGINAL_HISTORICAL_METRICS]
        ),
    }
    add_index_fields(marker, "COLLECTION", "COLLECTION")

    summary: Dict[str, Any] = {
        "instanceAlias": metadata.instance_alias,
        "instanceName": target.instance_name,
        "awsAccountId": target.account_id,
        "instanceId": target.instance_id,
        "collectionId": collection_id,
        "mode": "METRICS",
        "durationMs": duration_ms,
        "dataItemCount": len(items),
        "agentMetricCount": agent_metric_count,
        "queueMetricCount": queue_metric_count,
        "window": window,
        "rawAgentRowCount": int(metrics_result.get("rawAgentRowCount", 0)),
        "rawQueueRowCount": int(metrics_result.get("rawQueueRowCount", 0)),
        "metricRequestProfile": str(
            metrics_result.get("metricRequestProfile") or "ORIGINAL_INSTANCE_LAMBDA"
        ),
    }

    return CollectedInstance(
        target=target,
        collection_id=collection_id,
        data_items=items,
        marker_item=marker,
        response_summary=summary,
    )


def build_live_result_items(
    target: TargetInstance,
    metadata: InstanceMetadata,
    collection_id: str,
    live_result: Mapping[str, Any],
    collection_started: datetime,
    collection_finished: datetime,
) -> CollectedInstance:
    ttl_seconds = env_int("LIVE_RESULT_TTL_SECONDS", 120, minimum=1)
    expires_at = int(collection_finished.timestamp()) + ttl_seconds
    collected_at = iso_seconds(collection_finished)
    base = common_result_item(
        target,
        metadata,
        collection_id,
        collected_at,
        expires_at,
        collection_kind="LIVE",
        metric_source="GetCurrentMetricData+GetCurrentUserData",
    )
    items: List[Dict[str, Any]] = []

    status_item = {
        **base,
        "metricKey": "STATUS",
        "queueCount": int(live_result["queueCount"]),
        "totalContactsInQueue": int(live_result["totalContactsInQueue"]),
        "oldestContactAgeSeconds": int(live_result["oldestContactAgeSeconds"]),
        "agentTotal": int(sum(live_result["agentStates"].values())),
        "agentAvailable": int(live_result["agentStates"].get("Available", 0)),
        "agentStates": dict(live_result["agentStates"]),
        "pk": "STATUS",
        "sk": target.instance_id,
        "entityType": "STATUS",
        "entityId": target.instance_id,
    }
    add_index_fields(status_item, "STATUS", "STATUS")
    items.append(status_item)

    for queue_state in live_result["queueStates"]:
        queue_id = str(queue_state.get("queueId"))
        item = {
            **base,
            **dict(queue_state),
            "metricKey": f"QUEUE_STATE#{queue_id}",
            "snapshotTime": live_result.get("queueSnapshotTime"),
            "pk": "QUEUE_STATE",
            "sk": f"{target.instance_id}#{queue_id}",
            "entityType": "QUEUE_STATE",
            "entityId": queue_id,
        }
        add_index_fields(item, "QUEUE_STATE", f"{queue_state.get('queueName') or queue_id}#{queue_id}")
        items.append(item)

    duration_ms = int((collection_finished - collection_started).total_seconds() * 1000)
    marker = {
        **base,
        "metricKey": "LIVE_COLLECTION",
        "mode": "LIVE",
        "startedAt": iso_seconds(collection_started),
        "completedAt": collected_at,
        "durationMs": duration_ms,
        "dataItemCount": len(items),
        "queueStateCount": len(live_result["queueStates"]),
    }
    add_index_fields(marker, "LIVE_COLLECTION", "LIVE_COLLECTION")

    summary: Dict[str, Any] = {
        "instanceAlias": metadata.instance_alias,
        "instanceName": target.instance_name,
        "awsAccountId": target.account_id,
        "instanceId": target.instance_id,
        "collectionId": collection_id,
        "mode": "LIVE",
        "durationMs": duration_ms,
        "dataItemCount": len(items),
        "queueStateCount": len(live_result["queueStates"]),
        "totalContactsInQueue": int(live_result["totalContactsInQueue"]),
    }

    return CollectedInstance(
        target=target,
        collection_id=collection_id,
        data_items=items,
        marker_item=marker,
        response_summary=summary,
    )


def write_historical_instance(results_table: Any, collected: CollectedInstance) -> None:
    """Write historical rows then publish the fixed COLLECTION marker last."""

    validate_historical_items(collected.data_items)
    if collected.data_items:
        with results_table.batch_writer(
            overwrite_by_pkeys=["instanceKey", "metricKey"]
        ) as batch:
            for item in collected.data_items:
                batch.put_item(Item=ddb_safe(item))

    results_table.put_item(Item=ddb_safe(collected.marker_item))

    LOG.info(
        "Wrote historical results instance=%s data_items=%s",
        collected.target.configured_alias,
        len(collected.data_items),
    )


def write_live_instance(results_table: Any, collected: CollectedInstance) -> None:
    """Write live STATUS/QUEUE_STATE rows then the LIVE_COLLECTION marker."""

    if collected.data_items:
        with results_table.batch_writer(
            overwrite_by_pkeys=["instanceKey", "metricKey"]
        ) as batch:
            for item in collected.data_items:
                batch.put_item(Item=ddb_safe(item))

    results_table.put_item(Item=ddb_safe(collected.marker_item))

    LOG.info(
        "Wrote live results instance=%s data_items=%s",
        collected.target.configured_alias,
        len(collected.data_items),
    )


def debounce_allow(results_table: Any, target: TargetInstance) -> bool:
    """Lightweight conditional-write guard against overlapping live
    collections for the same instance when several raw Connect events land
    within a very short window. This is a soft guard, not a full dedupe
    mechanism -- if source event bursts are large, put an SQS (FIFO, one
    message group per instance) buffer ahead of this Lambda instead.
    """

    min_interval_seconds = env_int("LIVE_DEBOUNCE_SECONDS", 1, minimum=0)
    if min_interval_seconds <= 0:
        return True

    now_epoch = int(time.time())
    try:
        results_table.put_item(
            Item={
                "instanceKey": target.instance_key,
                "metricKey": "LIVE_LOCK",
                "lockedAt": now_epoch,
                "expiresAt": now_epoch + min_interval_seconds + 30,
                "recordType": "LIVE_LOCK",
            },
            ConditionExpression=(
                "attribute_not_exists(instanceKey) OR lockedAt < :cutoff"
            ),
            ExpressionAttributeValues={":cutoff": now_epoch - min_interval_seconds},
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


# ===========================================================================
# Event routing -- decide LIVE vs HISTORICAL, and resolve LIVE source identity
# ===========================================================================


def is_scheduled_event(event: Mapping[str, Any]) -> bool:
    return (
        str(event.get("source") or "").strip().lower() == "aws.events"
        and str(event.get("detail-type") or "").strip() == "Scheduled Event"
    )


def resolve_invocation_kind(event: Mapping[str, Any]) -> str:
    """Returns "LIVE" or "HISTORICAL"."""

    explicit = str(event.get("mode") or "").strip().upper()
    if explicit in ("LIVE", "METRICS"):
        return "LIVE" if explicit == "LIVE" else "HISTORICAL"

    if is_scheduled_event(event):
        return "HISTORICAL"

    # Any other EventBridge-shaped event (has a "detail" envelope key) is
    # assumed to be a Connect-originated live trigger. The EventBridge rule
    # wiring this Lambda to the ingestion bus is responsible for only
    # matching genuinely Connect-originated sources, so this Lambda does not
    # need to re-check the source string here.
    if "detail" in event:
        return "LIVE"

    # Direct/manual invocation with no envelope at all: preserve today's
    # default behaviour (full historical scan).
    return "HISTORICAL"


def _instance_id_from_ref(ref: str) -> str:
    if ":instance/" in ref:
        return ref.rsplit("/", 1)[-1]
    return ref


def extract_source_identity(event: Mapping[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Best-effort extraction of (account_id, instance_id) from a
    Connect-originated EventBridge event. See DETAIL_INSTANCE_REF_FIELDS /
    DETAIL_ACCOUNT_ID_FIELDS for the field names currently tried -- confirm
    against a real captured event and extend if the live path is not
    resolving instances correctly.
    """

    detail = event.get("detail")
    if not isinstance(detail, Mapping):
        detail = {}

    # Every EventBridge event carries the originating account at the
    # envelope level; this is the most reliable source of the account id.
    account_id = event.get("account") or first_present(detail, DETAIL_ACCOUNT_ID_FIELDS)

    instance_ref = first_present(detail, DETAIL_INSTANCE_REF_FIELDS)
    if instance_ref is None:
        return (str(account_id) if account_id else None, None)

    return (str(account_id) if account_id else None, _instance_id_from_ref(str(instance_ref)))


# ===========================================================================
# Per-invocation orchestration
# ===========================================================================


def collect_target_historical(
    target: TargetInstance,
    event: Mapping[str, Any],
    context: Any,
    event_bus_arn: str,
) -> CollectedInstance:
    remaining_time_check(context)
    collection_started = utc_now()
    collection_id = str(uuid.uuid4())
    started = time.perf_counter()

    instance_lookup_started = time.perf_counter_ns()
    connect = get_connect_client(target)
    metadata = get_instance_metadata(connect, target)
    instance_lookup_finished = time.perf_counter_ns()

    queue_lookup_started = time.perf_counter_ns()
    queue_ids, queue_map = list_standard_queues_cached(connect, target)
    queue_lookup_finished = time.perf_counter_ns()

    metrics_result = collect_historical_metrics(
        connect, target, metadata, queue_ids, queue_map, event
    )

    agent_detail, queue_detail = build_metrics_event_payloads(
        target,
        metadata,
        metrics_result,
        queue_lookup_us=(queue_lookup_finished - queue_lookup_started) // 1000,
        instance_lookup_us=(instance_lookup_finished - instance_lookup_started) // 1000,
    )
    publish_metrics_events(event_bus_arn, agent_detail, queue_detail)

    collection_finished = utc_now()
    collected = build_historical_result_items(
        target=target,
        metadata=metadata,
        collection_id=collection_id,
        metrics_result=metrics_result,
        collection_started=collection_started,
        collection_finished=collection_finished,
    )

    LOG.info(
        "Collected HISTORICAL instance=%s account=%s items=%s duration_ms=%s",
        metadata.instance_alias,
        target.account_id,
        len(collected.data_items),
        elapsed_ms(started),
    )
    return collected


def handle_historical_schedule(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    details_table_name = env_required("INSTANCE_DETAILS_TABLE_NAME")
    results_table_name = env_required("RESULTS_TABLE_NAME")
    event_bus_arn = env_required("CONTROLLER_EVENT_BUS_ARN")

    ddb = boto3.resource("dynamodb", region_name=MANAGEMENT_REGION, config=_BOTO_CONFIG)
    details_table = ddb.Table(details_table_name)
    results_table = ddb.Table(results_table_name)

    targets, configuration_errors = load_targets(details_table, event)
    if not targets and not configuration_errors:
        raise ConfigurationError("No enabled target instances were found")

    max_workers = min(
        env_int("MAX_INSTANCE_WORKERS", 4, minimum=1),
        max(1, len(targets)),
    )

    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = list(configuration_errors)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_targets = {
            executor.submit(
                collect_target_historical, target, event, context, event_bus_arn
            ): target
            for target in targets
        }

        for future in as_completed(future_targets):
            target = future_targets[future]
            try:
                collected = future.result()
                write_historical_instance(results_table, collected)
                successes.append(collected.response_summary)
            except Exception as exc:
                LOG.exception(
                    "Historical collection failed for instance=%s account=%s",
                    target.configured_alias,
                    target.account_id,
                )
                error_code = None
                if isinstance(exc, ClientError):
                    error_code = exc.response.get("Error", {}).get("Code")
                failure = {
                    "instanceAlias": target.configured_alias,
                    "instanceName": target.instance_name,
                    "awsAccountId": target.account_id,
                    "instanceId": target.instance_id,
                    "error": str(exc),
                }
                if error_code:
                    failure["errorCode"] = error_code
                failures.append(failure)

    successes.sort(key=lambda row: str(row["instanceAlias"]).lower())
    failures.sort(key=lambda row: str(row.get("instanceAlias", "")).lower())

    response: Dict[str, Any] = {
        "ok": not failures,
        "mode": "HISTORICAL",
        "targetCount": len(targets),
        "successCount": len(successes),
        "failureCount": len(failures),
        "resultsTableName": results_table_name,
        "successes": successes,
        "failures": failures,
    }

    fail_on_partial = bool_value(
        first_present(event, ("failOnPartialError", "fail_on_partial_error")),
        default=bool_value(os.getenv("FAIL_ON_PARTIAL_ERROR"), default=True),
    )
    if failures and fail_on_partial:
        LOG.error("historical partial failure: %s", json.dumps(response, default=str))
        raise PartialCollectionError(
            f"{len(failures)} of {len(targets) + len(configuration_errors)} configured "
            "instance rows failed; see CloudWatch logs for the structured result"
        )

    return serialisable(response)


def handle_live_trigger(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    details_table_name = env_required("INSTANCE_DETAILS_TABLE_NAME")
    results_table_name = env_required("RESULTS_TABLE_NAME")
    event_bus_arn = env_required("CONTROLLER_EVENT_BUS_ARN")

    account_id, instance_id = extract_source_identity(event)
    if not account_id or not instance_id:
        LOG.warning(
            "Could not resolve source account/instance from triggering event; "
            "skipping. event=%s",
            json.dumps(event, default=str)[:2000],
        )
        return {"ok": True, "path": "LIVE", "skipped": True, "reason": "unresolved-source-identity"}

    if account_id.isdigit():
        account_id = normalise_account_id(account_id)

    ddb = boto3.resource("dynamodb", region_name=MANAGEMENT_REGION, config=_BOTO_CONFIG)
    details_table = ddb.Table(details_table_name)
    results_table = ddb.Table(results_table_name)

    target = find_target_by_identity(details_table, account_id, instance_id)
    if target is None:
        LOG.warning(
            "No configured instance-details row for account=%s instance=%s; skipping",
            account_id,
            instance_id,
        )
        return {
            "ok": True,
            "path": "LIVE",
            "skipped": True,
            "reason": "instance-not-configured",
            "awsAccountId": account_id,
            "instanceId": instance_id,
        }

    if not debounce_allow(results_table, target):
        return {
            "ok": True,
            "path": "LIVE",
            "skipped": True,
            "reason": "debounced",
            "instanceAlias": target.configured_alias,
        }

    remaining_time_check(context)
    collection_started = utc_now()
    collection_id = str(uuid.uuid4())
    started = time.perf_counter()

    connect = get_connect_client(target)
    metadata = get_instance_metadata(connect, target)
    queue_ids, queue_map = list_standard_queues_cached(connect, target)

    live_result = collect_live_status(connect, target, queue_ids, queue_map)

    # Existing STATUS payload: unchanged shape.
    status_event_detail = {
        "instanceId": target.instance_id,
        "instanceAlias": metadata.instance_alias,
        "queueCount": int(live_result["queueCount"]),
        "totalContactsInQueue": int(live_result["totalContactsInQueue"]),
        "oldestContactAgeSeconds": int(live_result["oldestContactAgeSeconds"]),
        "agentTotal": int(sum(live_result["agentStates"].values())),
        "agentAvailable": int(live_result["agentStates"].get("Available", 0)),
        "agentStates": live_result["agentStates"],
        "timingsMs": {
            "total": live_result["msTotal"],
        },
        "schemaVersion": 1,
    }

    publish_status_event(event_bus_arn, status_event_detail)
    publish_queue_state_event(
        event_bus_arn=event_bus_arn,
        instance_id=target.instance_id,
        instance_alias=metadata.instance_alias,
        snapshot_time=live_result.get("queueSnapshotTime"),
        queue_states=live_result["queueStates"],
    )

    collection_finished = utc_now()
    collected = build_live_result_items(
        target=target,
        metadata=metadata,
        collection_id=collection_id,
        live_result=live_result,
        collection_started=collection_started,
        collection_finished=collection_finished,
    )
    write_live_instance(results_table, collected)

    LOG.info(
        "Collected LIVE instance=%s account=%s duration_ms=%s",
        metadata.instance_alias,
        target.account_id,
        elapsed_ms(started),
    )

    return {
        "ok": True,
        "path": "LIVE",
        "instanceAlias": target.configured_alias,
        "instanceId": target.instance_id,
        "awsAccountId": target.account_id,
        "durationMs": elapsed_ms(started),
        "eventPublished": True,
    }


def lambda_handler(event: Optional[Dict[str, Any]], context: Any) -> Dict[str, Any]:
    event = event or {}
    LOG.info("central connect-metrics invocation: %s", json.dumps(event, default=str)[:4000])

    kind = resolve_invocation_kind(event)

    if kind == "LIVE":
        return handle_live_trigger(event, context)

    return handle_historical_schedule(event, context)
