# IAM policy updates and EventBridge wiring for the central Connect metrics Lambda

## 1. Source-account role (the one `get-remote-metrics` assumes per instance)

This is the role referenced by each instance-details row's `assumeRoleArn` /
`assumeRoleName`. It previously only needed enough for historical metrics;
it now also needs the live/current-metrics actions and the read APIs used
for queue/user enrichment.

### Permissions policy (attach to the role, in the *source* account)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConnectHistoricalMetrics",
      "Effect": "Allow",
      "Action": [
        "connect:GetMetricDataV2"
      ],
      "Resource": "arn:aws:connect:REGION:SOURCE_ACCOUNT_ID:instance/INSTANCE_ID"
    },
    {
      "Sid": "ConnectLiveMetrics",
      "Effect": "Allow",
      "Action": [
        "connect:GetCurrentMetricData",
        "connect:GetCurrentUserData"
      ],
      "Resource": "arn:aws:connect:REGION:SOURCE_ACCOUNT_ID:instance/INSTANCE_ID"
    },
    {
      "Sid": "ConnectDirectoryReads",
      "Effect": "Allow",
      "Action": [
        "connect:DescribeInstance",
        "connect:ListQueues",
        "connect:ListUsers",
        "connect:DescribeUser"
      ],
      "Resource": [
        "arn:aws:connect:REGION:SOURCE_ACCOUNT_ID:instance/INSTANCE_ID",
        "arn:aws:connect:REGION:SOURCE_ACCOUNT_ID:instance/INSTANCE_ID/queue/*",
        "arn:aws:connect:REGION:SOURCE_ACCOUNT_ID:instance/INSTANCE_ID/agent/*"
      ]
    }
  ]
}
```

Notes:
- `GetMetricDataV2`, `GetCurrentMetricData` and `GetCurrentUserData` are
  instance-level actions in Connect's IAM action reference — the instance
  ARN alone is the correct resource, no sub-resource suffix needed.
- `ListQueues`/`ListUsers` return summaries for the instance; `DescribeUser`
  needs the `agent/*` resource pattern (Connect's IAM docs use `agent/*` for
  user/agent resources, not `user/*`).
- If you scope one role per instance today, keep doing that and just add
  these statements. If you use one shared role across several instances in
  the same account, replace `INSTANCE_ID` with `*` (or list each instance
  ARN explicitly, which is tighter).

### Trust policy (unchanged in shape, just confirm it still matches)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::MANAGEMENT_ACCOUNT_ID:role/CENTRAL_LAMBDA_EXECUTION_ROLE_NAME"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "PER_INSTANCE_EXTERNAL_ID_IF_USED"
        }
      }
    }
  ]
}
```

Drop the `Condition` block if you're not using `externalId` per instance
(the Lambda code already supports both).

## 2. Management-account Lambda execution role (the central Lambda itself)

Add to whatever execution role `get-remote-metrics` already runs as:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AssumeSourceAccountRoles",
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::*:role/ROLE_NAME_PATTERN_USED_ACROSS_SOURCE_ACCOUNTS"
    },
    {
      "Sid": "PublishToIngestionBus",
      "Effect": "Allow",
      "Action": "events:PutEvents",
      "Resource": "arn:aws:events:REGION:MANAGEMENT_ACCOUNT_ID:event-bus/INGESTION_BUS_NAME"
    },
    {
      "Sid": "ReadInstanceConfig",
      "Effect": "Allow",
      "Action": [
        "dynamodb:Scan",
        "dynamodb:GetItem"
      ],
      "Resource": "arn:aws:dynamodb:REGION:MANAGEMENT_ACCOUNT_ID:table/INSTANCE_DETAILS_TABLE_NAME"
    },
    {
      "Sid": "WriteResultsTable",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:BatchWriteItem"
      ],
      "Resource": "arn:aws:dynamodb:REGION:MANAGEMENT_ACCOUNT_ID:table/RESULTS_TABLE_NAME"
    }
  ]
}
```

(Plus the usual `logs:CreateLogGroup`/`CreateLogStream`/`PutLogEvents` for
CloudWatch, which is presumably already attached.)

`sts:AssumeRole` was almost certainly already present for the historical
path — the only genuinely new statement here is `events:PutEvents`, since
the old `get-remote-metrics` never published to the bus.

## 3. New environment variables the Lambda expects

| Variable | Used by | Notes |
|---|---|---|
| `CONTROLLER_EVENT_BUS_ARN` | both paths | Same ingestion bus this Lambda is triggered from. |
| `STATS_OFFSET_HOURS` | historical | Trailing window size in hours (default `12`, matches mgmt-supply's default). |
| `LIVE_RESULT_TTL_SECONDS` | live | DynamoDB TTL for STATUS/QUEUE_STATE rows (default `120`). |
| `LIVE_DEBOUNCE_SECONDS` | live | Minimum gap between live collections for the same instance (default `1`; set `0` to disable). |
| `MAX_INSTANCE_WORKERS` | historical | Unchanged from today. |
| everything else | both | Unchanged from the current `get-remote-metrics` deployment. |

## 4. EventBridge rule wiring (two independent triggers into one Lambda)

**Historical (schedule) trigger** — new, separate from the ingestion bus:
a plain EventBridge Scheduled Rule targeting this Lambda directly, e.g.
`rate(1 minute)`, with no meaningful input (`{}` is fine — the Lambda
detects `source: aws.events` / `detail-type: Scheduled Event` itself and
runs the full historical scan).

**Live trigger** — a rule *on the ingestion bus* that only matches
genuinely Connect-originated events, so this Lambda's own published events
(`ac.instance.*` sources) can never re-trigger it:

```json
{
  "source": [
    "aws.connect",
    "YOUR_KINESIS_RELAY_SOURCE_NAME"
  ]
}
```

Replace `YOUR_KINESIS_RELAY_SOURCE_NAME` with whatever `Source` value the
pipe/Lambda that forwards Agent Event Stream (Kinesis) records onto the bus
actually sets — I don't have a captured sample of that event, so the field
names `extract_source_identity()` looks for in `get-remote-metrics.py`
(`instanceArn`/`InstanceARN`/`instanceId`/…) are a best-effort list. Once
you have a real sample event, check it against `DETAIL_INSTANCE_REF_FIELDS`
/ `DETAIL_ACCOUNT_ID_FIELDS` near the top of the file and extend those
tuples if the actual field name isn't already covered — everything else
downstream (role lookup, live collection, publishing) is unaffected by
that field name.
