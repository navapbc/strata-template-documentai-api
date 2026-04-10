class ResponseCodes:
    SUCCESS = "000"
    BITMAP_RECEIVED = "001"
    DOCUMENT_TYPE_NOT_IMPLEMENTED = "002"
    MISSING_FIELDS = "101"
    NO_DOCUMENT_DETECTED = "103"
    BLURRY_DOCUMENT_DETECTED = "104"
    MULTIPAGE_DOCUMENT = "400"
    INTERNAL_PROCESSING_ERROR = "999"

    @classmethod
    def get_message(cls, code: str) -> str:
        """Get message for response code."""
        messages = {
            cls.SUCCESS: "Document validation passed",
            cls.BITMAP_RECEIVED: "Bitmap received",
            cls.DOCUMENT_TYPE_NOT_IMPLEMENTED: "Document type not implemented",
            cls.MISSING_FIELDS: "Missing fields",
            cls.NO_DOCUMENT_DETECTED: "No document detected",
            cls.BLURRY_DOCUMENT_DETECTED: "Document is blurry",
            cls.MULTIPAGE_DOCUMENT: "Multi-page document",
            cls.INTERNAL_PROCESSING_ERROR: "Internal processing error",
        }
        return messages.get(code, "")

    @classmethod
    def is_success_response_code(cls, code: str) -> bool:
        """Get message for response code."""
        return code.startswith("0")
