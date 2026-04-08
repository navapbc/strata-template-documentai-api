from documentai_api.mappings.document_classes import TEXTRACT_ID_TYPE_TO_BDA_DOCUMENT_CLASS
from documentai_api.mappings.us_passports import TEXTRACT_TO_BDA_FIELD_MAP as US_PASSPORT_FIELD_MAP
from documentai_api.mappings.us_drivers_licenses import TEXTRACT_TO_BDA_FIELD_MAP as US_DL_FIELD_MAP

_FIELD_MAPS = {
    "US-passports": US_PASSPORT_FIELD_MAP,
    "US-drivers-licenses": US_DL_FIELD_MAP,
}

def map_textract_to_bda_fields(fields: dict, document_class: str) -> dict:
    field_map = _FIELD_MAPS.get(document_class, {})
    return {
        bda_name: fields.get(textract_name, {"confidence": 0, "value": ""})
        for textract_name, bda_name in field_map.items()
    }

def get_document_class(textract_id_type: str) -> str | None:
    return TEXTRACT_ID_TYPE_TO_BDA_DOCUMENT_CLASS.get(textract_id_type)
