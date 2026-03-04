import typer

from documentai_api.jobs.document_processor.main import main

app = typer.Typer()


@app.command()
def cli(
    object_key: str = typer.Argument(..., help="S3 object key (e.g. 'input/document.pdf')"),
    bucket_name: str | None = typer.Argument(
        None, help="S3 bucket name (defaults to DOCUMENTAI_INPUT_LOCATION env var)"
    ),
):
    """Process uploaded document and invoke BDA."""
    try:
        main(object_key, bucket_name)
    except Exception:
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
