# Deployment to cloud environments

## With Strata AWS Infrastructure Template

This template can be deployed using the [Nava Strata AWS Infrastructure
Template](https://github.com/navapbc/template-infra). Using the infrastructure
template will handle creating and configuring most of the resources required in
AWS.

While following the [infrastructure template installation
instructions](https://github.com/navapbc/template-infra?tab=readme-ov-file#installation)
and [setup
instructions](https://github.com/navapbc/template-infra/blob/main/infra/README.md),
use the following configuration:

1. Be sure you've installed this template and the infra-app template via [the
   nava-platform tool](https://github.com/navapbc/platform-cli) using the same
   `<APP_NAME>`.
1. In `/infra/<APP_NAME>/app-config/main.tf`:
    1. Set `has_database` to `false`.
    2. Set `enable_document_data_extraction` to `true`.
1. Configure the Document Data Extraction module with custom blueprints or other
   configuration tweaks following [the
   docs](https://github.com/navapbc/template-infra/blob/main/docs/infra/document-data-extraction.md).
    1. Things will work out of the box, but understand the update process noted
       in the docs for tuning behavior after initial setup.
1. In `/infra/<APP_NAME>/app-config/env-config/file_upload_jobs.tf`:
    1. Configure the service's `file_upload_jobs`:

    ```terraform
    document_processor = {
      source_bucket = local.document_data_extraction_config != null ? local.document_data_extraction_config.input_bucket_name : null
      path_prefix   = "input/"
      task_command  = ["document_processor", "<object_key>", "<bucket_name>"]
    }
    bda_result_processor = {
      source_bucket = local.document_data_extraction_config != null ? local.document_data_extraction_config.output_bucket_name : null
      path_prefix   = "processed/"
      task_command  = ["bda_result_processor", "<bucket_name>", "<object_key>"]
    }
    ```

1. Configure DynamoDB
    1. In `/infra/project-config/aws_services.tf`:
        1. Add `dynamodb` to `aws_services` if not already present.
    1. Create `infra/<APP_NAME>/service/documentai_api.tf` with:

    ```terraform
    # documentai_api.tf - App-specific resources for DocumentAI API

    data "aws_caller_identity" "current" {}

    locals {
      documentai_api_document_metadata_table_name = "${local.service_name}-document-metadata"
      documentai_api_db_job_id_index_name         = "jobId-index"

      documentai_api_environment_variables = merge(
        {
          DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME        = aws_dynamodb_table.documentai_api_document_metadata.name
          DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME = local.documentai_api_db_job_id_index_name

          ENVIRONMENT = var.environment_name
        },
        local.document_data_extraction_config != null ? {
          # Alias standard DDE env vars
          #
          # TODO(https://github.com/navapbc/strata-template-documentai-api/issues/52)
          # the app could respect the DDE_ versions
          DOCUMENTAI_INPUT_LOCATION  = "${local.document_data_extraction_environment_variables.DDE_INPUT_LOCATION}/input"
          DOCUMENTAI_OUTPUT_LOCATION = "${local.document_data_extraction_environment_variables.DDE_OUTPUT_LOCATION}/processed"
          BDA_PROJECT_ARN            = local.document_data_extraction_environment_variables.DDE_PROJECT_ARN
          BDA_PROFILE_ARN            = local.document_data_extraction_environment_variables.DDE_PROFILE_ARN

          # TODO(https://github.com/navapbc/strata-template-documentai-api/issues/53)
          # this could be extracted from BDA_PROFILE_ARN or similar and not require
          # separate configuration
          BDA_REGION = local.document_data_extraction_config.bda_region
      } : {})

      documentai_api_document_metadata_table_arn = aws_dynamodb_table.documentai_api_document_metadata.arn
    }

    # KMS Key for DynamoDB Encryption
    data "aws_iam_policy_document" "documentai_api_dynamodb_kms_key_policy" {
      # checkov:skip=CKV_AWS_109:Root account requires full KMS permissions to enable IAM-based access control
      # checkov:skip=CKV_AWS_111:Root account requires full KMS permissions to enable IAM-based access control
      # checkov:skip=CKV_AWS_356:In a key policy, the wildcard character in the Resource element represents the KMS key to which the key policy is attached.

      statement {
        sid    = "Enable IAM User Permissions"
        effect = "Allow"
        principals {
          type        = "AWS"
          identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
        }
        actions   = ["kms:*"]
        resources = ["*"]
      }
    }

    resource "aws_kms_key" "documentai_api_dynamodb" {
      description             = "KMS key for DocumentAI DynamoDB tables"
      deletion_window_in_days = 10
      enable_key_rotation     = true
      policy                  = data.aws_iam_policy_document.documentai_api_dynamodb_kms_key_policy.json
    }

    resource "aws_dynamodb_table" "documentai_api_document_metadata" {
      name         = local.documentai_api_document_metadata_table_name
      billing_mode = "PAY_PER_REQUEST"
      hash_key     = "fileName"

      attribute {
        name = "fileName"
        type = "S"
      }

      attribute {
        name = "jobId"
        type = "S"
      }

      ttl {
        attribute_name = "ttl"
        enabled        = true
      }

      global_secondary_index {
        name            = local.documentai_api_db_job_id_index_name
        hash_key        = "jobId"
        projection_type = "ALL"
      }

      server_side_encryption {
        enabled     = true
        kms_key_arn = aws_kms_key.documentai_api_dynamodb.arn
      }

      point_in_time_recovery {
        enabled = true
      }

      deletion_protection_enabled = !local.is_temporary
    }

    resource "aws_iam_policy" "documentai_api_dynamodb_read_write" {
      name   = "${local.service_name}-dynamodb-access"
      policy = data.aws_iam_policy_document.documentai_api_dynamodb_read_write.json
    }

    data "aws_iam_policy_document" "documentai_api_dynamodb_read_write" {
      statement {
        actions = [
          "dynamodb:BatchWriteItem",
          "dynamodb:DeleteItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:BatchGetItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:DescribeTable"
        ]
        resources = [
          local.documentai_api_document_metadata_table_arn,
          "${local.documentai_api_document_metadata_table_arn}/index/*"
        ]
        effect = "Allow"

      }
      statement {
        actions = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
        ]
        resources = [aws_kms_key.documentai_api_dynamodb.arn]
        effect    = "Allow"
      }
    }
    ```

    1. In `infra/<APP_NAME>/service/main.tf` add:
        1. Update `extra_environment_variables` to add:

        ```terraform
        local.documentai_api_environment_variables,
        ```

        1. Update `extra_policies` to add:

        ```terraform
        {
            documentai_api_dynamodb_access = aws_iam_policy.dynamodb_read_write.arn,
        },
        ```

1. In `/infra/<APP_NAME>/app-config/env-config/environment_variables.tf`:
    1. Add an entry to `secrets`:

    ```terraform
    API_AUTH_INSECURE_SHARED_KEY = {
      manage_method     = "generated"
      secret_store_name = "/${var.app_name}-${var.environment}/api-auth-insecure-shared-key"
    }
    ```

1. Follow the infrastructure template instructions to configure [custom
   domains](https://github.com/navapbc/template-infra/blob/main/docs/infra/set-up-custom-domains.md)
   and [https
   support](https://github.com/navapbc/template-infra/blob/main/docs/infra/https-support.md).

## General

At minimum you will need to set up:

- Container runtime environment for the application server and background jobs
- S3 bucket(s)
- A means to trigger the data processing jobs based on files created in specific
  S3 location
- DynamoDB table
- Bedrock Data Automation project

With the container runtime environment(s) configured for the created resources
according to `/template/{{app_name}}/README.md.jinja` in the "Configuration"
section.
