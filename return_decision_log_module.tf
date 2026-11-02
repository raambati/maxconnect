# Replace the existing stub module block for the decision-log read function in lambda.tf
# (the one that currently deploys mc-dev-return-decision-log) with this block, and replace
# the stub code with:  code/return_decision_log/return_decision_log.py
# Keep the module's name and function_name the same as the stub so the ALB target does not change.
# COPY THIS FROM THE FILE, NOT FROM THE CHAT (hidden characters broke lambda.tf last time).

module "return_decision_log" {
  source                = "./AC_Lambda"
  code_bucket           = var.code_bucket
  function_name         = "${var.prefix}-return-decision-log"   # match the stub's existing name
  code_path             = "../AC_MC_Phase2/code/return_decision_log"
  runtime               = "python3.14"
  handler               = "return_decision_log.lambda_handler"
  register_with_connect = false
  prefix                = var.prefix
  environment_id        = "dev"
  kms_alias             = "alias/aws/dynamodb"
  enable_logs_kms       = false
  timeout               = 15
  memory_size           = 256
  environment_variables = {
    DECISION_LOG_TABLE_NAME = aws_dynamodb_table.decision_log.name
    ALLOWED_ORIGIN          = "*"      # set to the console's origin once Jacob confirms it
    MAX_SCAN_ITEMS          = "2000"
  }
  custom_role_json = {
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "ReadDecisionLog",
        "Effect" : "Allow",
        "Action" : ["dynamodb:Query", "dynamodb:Scan"],
        "Resource" : [
          aws_dynamodb_table.decision_log.arn,
          "${aws_dynamodb_table.decision_log.arn}/index/*"
        ]
      },
      {
        "Sid" : "KmsForEncryptedTables",
        "Effect" : "Allow",
        "Action" : ["kms:Decrypt"],
        "Resource" : [
          data.aws_kms_key.this.arn,
          aws_kms_key.this.arn
        ]
      }
    ]
  }
}
