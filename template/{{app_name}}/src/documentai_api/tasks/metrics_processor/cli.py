#!/usr/bin/env python3
"""CLI for metrics processor."""

import typer

from documentai_api.tasks.metrics_processor.main import main
from documentai_api.utils import env

app = typer.Typer()


@app.command()
def process(
    queue_url: str = typer.Option(
        ..., "--queue-url", envvar=env.DOCUMENTAI_METRICS_QUEUE_URL, help="SQS queue URL"
    ),
    bucket_name: str = typer.Option(
        ...,
        "--bucket-name",
        envvar=env.DOCUMENTAI_METRICS_BUCKET_NAME,
        help="S3 bucket for metrics",
    ),
    max_messages: int = typer.Option(10, "--max-messages", help="Max messages per batch"),
    max_batches: int = typer.Option(10, "--max-batches", help="Max batches to process"),
):
    """Process metrics from SQS queue and write to S3."""
    result = main(queue_url, bucket_name, max_messages, max_batches)
    typer.echo(f"Processed {result} messages")


if __name__ == "__main__":
    app()
