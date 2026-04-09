class DocumentMetadata:
    # core fields
    FILE_NAME = "fileName"
    ORIGINAL_FILE_NAME = "originalFileName"
    USER_PROVIDED_DOCUMENT_CATEGORY = "userProvidedDocumentCategory"
    PROCESS_STATUS = "processStatus"
    BDA_INVOCATION_ARN = "bdaInvocationArn"
    BDA_OUTPUT_S3_URI = "bdaOutputS3Uri"
    ERROR_MESSAGE = "errorMessage"
    RESPONSE_JSON = "responseJson"
    RESPONSE_CODE = "responseCode"
    PROCESSED_DATE = "processedDate"
    JOB_ID = "jobId"
    TRACE_ID = "traceId"
    V1_API_RESPONSE_JSON = "v1ApiResponseJson"
    CREATED_AT = "createdAt"
    UPDATED_AT = "updatedAt"

    # performance tracking
    BDA_STARTED_AT = "bdaStartedAt"
    BDA_COMPLETED_AT = "bdaCompletedAt"
    TOTAL_PROCESSING_TIME_SECONDS = "totalProcessingTimeSeconds"
    BDA_PROCESSING_TIME_SECONDS = "bdaProcessingTimeSeconds"  # time bda took to process the file
    BDA_WAIT_TIME_SECONDS = "bdaWaitTimeSeconds"  # time between s3 write and bda invocation

    # file metadata
    FILE_SIZE_BYTES = "fileSizeBytes"
    CONTENT_TYPE = "contentType"
    PAGES_DETECTED = "pagesDetected"
    IS_DOCUMENT_BLURRY = "isDocumentBlurry"
    IS_PASSWORD_PROTECTED = "isPasswordProtected"
    DOCUMENT_METRICS_RAW = "documentMetricsRaw"
    DOCUMENT_METRICS_NORMALIZED = "documentMetricsNormalized"
    OVERALL_BLUR_SCORE = "overallBlurScore"

    # operational intelligence
    ADDITIONAL_INFO = "additionalInfo"
    RETRY_COUNT = "retryCount"
    FIELD_CONFIDENCE_SCORES = "fieldConfidenceScores"

    # bda processing info
    BDA_REGION_USED = "bdaRegionUsed"
    BDA_MATCHED_BLUEPRINT_NAME = "matchedBlueprintName"
    BDA_MATCHED_BLUEPRINT_CONFIDENCE = "matchedBlueprintConfidence"
    BDA_MATCHED_DOCUMENT_CLASS = "bdaMatchedDocumentClass"

    # list of blueprint fields that were expected but did not have any data extracted
    BDA_MATCHED_BLUEPRINT_FIELD_EMPTY_LIST = "matchedBlueprintFieldEmptyList"
    BDA_MATCHED_BLUEPRINT_FIELD_BELOW_THRESHOLD_LIST = "matchedBlueprintFieldBelowThresholdList"
    BDA_MATCHED_BLUEPRINT_FIELD_COUNT = "matchedBlueprintFieldCount"
    BDA_MATCHED_BLUEPRINT_FIELD_COUNT_NOT_EMPTY = "matchedBlueprintFieldCountNotEmpty"
    BDA_MATCHED_BLUEPRINT_FIELD_NOT_EMPTY_AVG_CONFIDENCE = (
        "matchedBlueprintFieldNotEmptyAvgConfidence"
    )
