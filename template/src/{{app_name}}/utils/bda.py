from config.constants import BDA_JOB_STATUS_RUNNING, BDA_JOB_STATUS_FAILED, BDA_JOB_STATUS_COMPLETED

def is_bda_job_running(status: str) -> bool:
    """Check if BDA job is still running"""
    return status in BDA_JOB_STATUS_RUNNING

def is_bda_job_failed(status: str) -> bool:
    """Check if BDA job has failed"""
    return status in BDA_JOB_STATUS_FAILED

def is_bda_job_completed(status: str) -> bool:
    """Check if BDA job completed successfully"""
    return status in BDA_JOB_STATUS_COMPLETED

def extract_values_from_bda_results(bda_results):
    """Extract field values from BDA results"""
    if not bda_results or "explainability_info" not in bda_results:
        return {}
    
    field_values = {}
    for item in bda_results["explainability_info"]:
        if isinstance(item, dict):
            for field_name, field_data in item.items():
                if isinstance(field_data, dict):
                    field_values[field_name] = field_data.get("field_value", "")
    
    return field_values
