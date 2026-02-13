import json

import typer

from documentai_api.tasks.bda_invoker.main import main

app = typer.Typer()


@app.command()
def cli(
    file_name: str = typer.Option(..., help="Name of file to process"),
    bucket_name: str | None = typer.Option(
        None, help="S3 bucket name (defaults to DDE_INPUT_LOCATION env var)"
    ),
    bypass_ddb_status_check: bool = typer.Option(False, help="Skip checking DDB record status"),
):
    try:
        result = main(file_name, bucket_name, bypass_ddb_status_check)
        if result:
            typer.echo(json.dumps(result))
    except Exception:
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
