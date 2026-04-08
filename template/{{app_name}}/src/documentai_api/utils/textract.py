from documentai_api.utils.logger import get_logger
from documentai_api.utils.models import ExtractedFieldResult, ExtractedFieldResultsSummary
logger = get_logger(__name__)

# Map Textract field types to normalized field names
FIELD_TYPE_MAP = {
    "FIRST_NAME": "firstName",
    "LAST_NAME": "lastName",
    "MIDDLE_NAME": "middleName",
    "DOCUMENT_NUMBER": "documentNumber",
    "EXPIRATION_DATE": "expirationDate",
    "DATE_OF_BIRTH": "dateOfBirth",
    "DATE_OF_ISSUE": "dateOfIssue",
    "ID_TYPE": "idType",
    "ADDRESS": "address",
    "STATE_IN_ADDRESS": "state",
    "CITY_IN_ADDRESS": "city",
    "ZIP_CODE_IN_ADDRESS": "zipCode",
    "STATE_NAME": "stateName",
    "PLACE_OF_BIRTH": "placeOfBirth",
    "SEX": "sex",
    "MRZ_CODE": "mrzCode",
    "CLASS": "licenseClass",
    "RESTRICTIONS": "restrictions",
    "ENDORSEMENTS": "endorsements",
}


def extract_fields_from_analyze_id(response: dict) -> dict:
    """Extract structured fields from Textract AnalyzeID response.

    Returns dict of {field_name: {"confidence": float, "value": str}}
    """
    fields = {}

    for doc in response.get("IdentityDocuments", []):
        for field in doc.get("IdentityDocumentFields", []):
            field_type = field.get("Type", {}).get("Text", "")
            value_detection = field.get("ValueDetection", {})
            value = value_detection.get("Text", "")
            confidence = value_detection.get("Confidence", 0.0)

            field_name = FIELD_TYPE_MAP.get(field_type, field_type.lower())

            # use normalized value for dates if available
            normalized = value_detection.get("NormalizedValue", {})
            if normalized.get("Value"):
                value = normalized["Value"]

            fields[field_name] = {
                "confidence": round(confidence / 100.0, 2),
                "value": value,
            }

    return fields


def get_id_type(response: dict) -> str | None:
    """Extract ID type from AnalyzeID response."""
    for doc in response.get("IdentityDocuments", []):
        for field in doc.get("IdentityDocumentFields", []):
            if field.get("Type", {}).get("Text") == "ID_TYPE":
                return field.get("ValueDetection", {}).get("Text")
    return None


def extract_field_values_from_textract_results(result_json: dict) -> tuple[ExtractedFieldResultsSummary, dict]:
    fields = result_json.get("fields", {})
    
    confidence_scores = []
    empty_fields = []
    field_confidence_map_list = []
    field_values = {}

    for name, data in fields.items():
        conf = data["confidence"]
        value = data.get("value", "")
        
        field_confidence_map_list.append({name: conf})
        
        if not value:
            empty_fields.append(name)
        else:
            confidence_scores.append(conf)
        
        field_values[name] = value

    metadata = ExtractedFieldResultsSummary(
        confidence_scores=confidence_scores,
        empty_fields=empty_fields,
        field_confidence_map_list=field_confidence_map_list,
    )
    return metadata, field_values
