#!/usr/bin/env python3
"""CLI for daily metrics aggregator."""

import typer

from documentai_api.tasks.metrics_aggregator.main import main

app = typer.Typer()


@app.command()
def aggregate(
    target_date: str = typer.Argument(..., help="Date to aggregate (YYYY-MM-DD)"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Re-aggregate even if stats exist"),
):
    """Aggregate metrics for a specific date."""
    result = main(target_date, overwrite)
    typer.echo(f"Aggregation complete: {result}")


if __name__ == "__main__":
    app()
