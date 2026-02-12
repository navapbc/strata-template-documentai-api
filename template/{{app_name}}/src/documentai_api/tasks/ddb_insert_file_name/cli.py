import typer

from documentai_api.tasks.ddb_insert_file_name.main import main


def cli(
    bucket_name: str = typer.Option(..., help="S3 bucket name"),
    object_key: str = typer.Option(..., help="S3 object key"),
    user_provided_document_category: str | None = typer.Option(
        None, help="User provided document category"
    ),
    job_id: str | None = typer.Option(None, help="Job ID"),
    trace_id: str | None = typer.Option(None, help="Trace ID"),
):
    try:
        main(bucket_name, object_key, user_provided_document_category, job_id, trace_id)
    except Exception:
        raise typer.Exit(1) from None


if __name__ == "__main__":
    typer.run(cli)
