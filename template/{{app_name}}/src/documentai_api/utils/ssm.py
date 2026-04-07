"""SSM Parameter Store helpers with caching."""

from documentai_api.services import ssm as ssm_service
from documentai_api.utils.cache import get_cache
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)

_SSM_CACHE_TTL_MINUTES = 5


def get_parameter_value(param_name: str, default: str | None = None) -> str:
    """Get SSM parameter with caching."""
    cache = get_cache()
    cached = cache.get(f"ssm:{param_name}")
    if cached is not None:
        return cached

    try:
        value = ssm_service.get_parameter(param_name)
        cache.add(f"ssm:{param_name}", value, _SSM_CACHE_TTL_MINUTES)
        return value
    except Exception as e:
        logger.error(f"Failed to get parameter {param_name}: {e}")
        if default is not None:
            return default
        raise
