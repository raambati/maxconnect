# Replace the existing module "supply_calc" block in lambda.tf with this one.
# Code folder stays: code/supply_calc/supply_calc.py  (replace the stub file).

module "supply_calc" {
  source                = "./AC_Lambda"
  code_bucket           = var.code_bucket
  function_name         = "${var.prefix}-supply-calc"
  code_path             = "../AC_MC_Phase2/code/supply_calc"
  runtime               = "python3.14"
  handler               = "supply_calc.lambda_handler"
  register_with_connect = false
  prefix                = var.prefix
  environment_id        = "dev"
  kms_alias             = "alias/aws/dynamodb"
  enable_logs_kms       = false
  timeout               = 30
  memory_size           = 256
  environment_variables = {
    AGENT_STATE_TABLE_NAME      = "macp-uk-mgmt-dev-agent-state"
    QUEUE_STATE_TABLE_NAME      = "macp-uk-mgmt-dev-queue-state"
    PRIORITY_SCORES_TABLE_NAME  = aws_dynamodb_table.priority_scores.name
    CONFIG_TABLE_NAME           = aws_dynamodb_table.config.name
    DECISION_LOG_TABLE_NAME     = aws_dynamodb_table.decision_log.name
    MAX_QUEUE_STATE_AGE_SECONDS = "180"
    MANDATORY_BYPASSES_FLEX_CAP = "false"
  }
  custom_role_json = {
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "ReadAgentState",
        "Effect" : "Allow",
        "Action" : ["dynamodb:Scan"],
        "Resource" : "arn:aws:dynamodb:eu-west-2:919484652802:table/macp-uk-mgmt-dev-agent-state"
      },
      {
        "Sid" : "ReadQueueState",
        "Effect" : "Allow",
        "Action" : ["dynamodb:BatchGetItem"],
        "Resource" : "arn:aws:dynamodb:eu-west-2:919484652802:table/macp-uk-mgmt-dev-queue-state"
      },
      {
        "Sid" : "ReadPriorityAndConfig",
        "Effect" : "Allow",
        "Action" : ["dynamodb:GetItem"],
        "Resource" : [
          aws_dynamodb_table.priority_scores.arn,
          aws_dynamodb_table.config.arn
        ]
      },
      {
        "Sid" : "WriteDecisionLog",
        "Effect" : "Allow",
        "Action" : ["dynamodb:PutItem"],
        "Resource" : aws_dynamodb_table.decision_log.arn
      },
      {
        "Sid" : "KmsForEncryptedTables",
        "Effect" : "Allow",
        "Action" : ["kms:Decrypt", "kms:GenerateDataKey"],
        # If the two Phase 1 tables (agent-state, queue-state) use a different
        # KMS key from the Phase 2 tables, add that key's ARN to this list.
        "Resource" : data.aws_kms_key.this.arn
      }
    ]
  }
}
