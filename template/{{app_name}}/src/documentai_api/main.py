import uvicorn

import documentai_api.logging
from documentai_api.config.env import get_app_env_config


def main() -> None:  # pragma: no cover
    with documentai_api.logging.init(__package__):
        config = get_app_env_config()
        uvicorn.run(
            "documentai_api.app:app",
            host=config.host,
            port=config.port,
            reload=False,
            log_config=None,
        )


if __name__ == "__main__":
    main()
