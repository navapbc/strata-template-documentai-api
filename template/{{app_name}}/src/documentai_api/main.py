import os

import uvicorn


def main():
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", 8000))

    uvicorn.run("documentai_api.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
