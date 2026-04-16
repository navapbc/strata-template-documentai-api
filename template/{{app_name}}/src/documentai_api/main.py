import os

import uvicorn

from documentai_api.logging.config import LoggingContext


def main() -> None:  # pragma: no cover
    with LoggingContext(__package__):
        host = os.getenv("HOST", "127.0.0.1")
        port = int(os.getenv("PORT", 8000))

        uvicorn.run("documentai_api.app:app", host=host, port=port, reload=False, log_config=None)


if __name__ == "__main__":
    main()
