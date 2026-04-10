import json
from typing import Annotated

import typer

app = typer.Typer()


@app.command()
def export_openapi(
    output: Annotated[
        typer.FileTextWrite, typer.Option(help="File to write to, or '-' for stdout")
    ] = "-",  # type: ignore[assignment]
) -> None:
    """Export OpenAPI specification."""
    from documentai_api.app import app as fastapi_app

    spec = json.dumps(fastapi_app.openapi(), indent=2)
    output.write(spec)


if __name__ == "__main__":
    app()
