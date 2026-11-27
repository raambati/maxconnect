# Layer 3 demand counters: how many contacts have entered each queue today and this month.
# One item per queue per day, and one per queue per month:
#   PK = QUEUE#<canonical queue key>   e.g. QUEUE#carers_uk_tel
#   SK = DAY#2026-11-18   or   MONTH#2026-11
#   contacts = running total (incremented with an atomic ADD every time a contact enters the queue)
#   expiresAt = epoch seconds; DynamoDB deletes old items by itself (TTL)
#
# COPY THE ENCRYPTION / TAGS FROM THE decision_log TABLE IN dynamodb.tf so it matches your other tables.
# COPY THIS FROM THE FILE, NOT FROM THE CHAT (hidden characters broke lambda.tf once).

resource "aws_dynamodb_table" "demand_counters" {
  name         = "${var.prefix}-demand-counters"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }
  attribute {
    name = "SK"
    type = "S"
  }

  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.this.arn
  }

  point_in_time_recovery {
    enabled = true
  }
}
