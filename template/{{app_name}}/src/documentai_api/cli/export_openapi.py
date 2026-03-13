import json

import typer

app = typer.Typer()


@app.command()
def export_openapi(
    output: str = typer.Option(None, help="Output file path. If not provided, prints to stdout."),
):
    """Export OpenAPI specification."""
    from documentai_api.app import app as fastapi_app

    spec = json.dumps(fastapi_app.openapi(), indent=2)

    if output:
        with open(output, "w") as f:
            f.write(spec)
        typer.echo(f"OpenAPI spec written to {output}")
    else:
        print(spec)


if __name__ == "__main__":
    app()
