import json

import typer

from documentai_api.tasks.bda_output_processor.main import main

app = typer.Typer()


@app.command()
def cli(
    bucket_name: str = typer.Option(..., help="S3 bucket containing BDA output"),
    object_key: str = typer.Option(..., help="S3 object key of BDA output file"),
):
    try:
        result = main(bucket_name, object_key)
        if result:
            typer.echo(json.dumps(result))
    except Exception:
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
