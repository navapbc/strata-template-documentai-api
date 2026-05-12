"""CLI for managing API keys."""

from datetime import UTC, datetime
from typing import Annotated

import typer

app = typer.Typer()


@app.command()
def generate(
    client_name: Annotated[str, typer.Option(help="Name of the calling system")],
    environment: Annotated[str, typer.Option(help="Deployment environment (e.g. prod, staging)")],
    expires_at: Annotated[
        str | None,
        typer.Option(
            help="Optional expiry date in ISO 8601 format (e.g. 2027-01-01T00:00:00+00:00)"
        ),
    ] = None,
) -> None:
    """Generate an API key, store its hash in DynamoDB, and print the plaintext key.

    The plaintext key is shown once and never stored. Store it securely.
    """
    from documentai_api.utils.auth import generate_api_key

    parsed_expires_at = None
    if expires_at:
        try:
            parsed_expires_at = datetime.fromisoformat(expires_at)
            if parsed_expires_at.tzinfo is None:
                parsed_expires_at = parsed_expires_at.replace(tzinfo=UTC)
        except ValueError:
            typer.echo(f"Error: Invalid expires_at format: {expires_at}", err=True)
            typer.echo("Expected ISO 8601 format, e.g. 2027-01-01T00:00:00+00:00", err=True)
            raise typer.Exit(code=1) from None

    try:
        api_key, existing_keys = generate_api_key(
            client_name=client_name,
            environment=environment,
            expires_at=parsed_expires_at,
        )
    except Exception as e:
        typer.echo(f"Error: Failed to generate API key: {e}", err=True)
        raise typer.Exit(code=1) from None

    if existing_keys:
        typer.echo("")
        typer.echo(
            f"Warning: {len(existing_keys)} active key(s) already exist for client '{client_name}'.",
            err=True,
        )
        typer.echo(
            "The old key(s) remain active. Deactivate them once the client has migrated.",
            err=True,
        )

    typer.echo("")
    typer.echo("API Key (save this — it will not be shown again):")
    typer.echo(f"  {api_key}")
    typer.echo("")
    typer.echo(f"Client:      {client_name}")
    typer.echo(f"Environment: {environment}")  # for record-keeping only, not embedded in key
    if parsed_expires_at:
        typer.echo(f"Expires:     {parsed_expires_at.isoformat()}")
    else:
        typer.echo("Expires:     never")
    typer.echo("")


@app.command()
def deactivate(
    client_name: Annotated[str, typer.Option(help="Name of the calling system")],
    api_key: Annotated[str | None, typer.Option(help="Plaintext API key to deactivate")] = None,
    all_keys: Annotated[
        bool, typer.Option("--all", help="Deactivate all active keys for the client")
    ] = False,
) -> None:
    """Deactivate one or all active API keys for a client."""
    from documentai_api.utils.auth import _hash_key, deactivate_api_key, get_active_keys_for_client

    if not api_key and not all_keys:
        typer.echo("Error: Provide --api-key or --all", err=True)
        raise typer.Exit(code=1) from None

    if api_key and all_keys:
        typer.echo("Error: Provide --api-key or --all, not both", err=True)
        raise typer.Exit(code=1) from None

    if api_key:
        key_hash = _hash_key(api_key)
        deactivated = deactivate_api_key(key_hash)
        if deactivated:
            typer.echo(f"Deactivated key for client: {client_name}")
        else:
            typer.echo(f"Error: Key not found for client: {client_name}", err=True)
            raise typer.Exit(code=1) from None
    else:
        active_keys = get_active_keys_for_client(client_name)
        if not active_keys:
            typer.echo(f"No active keys found for client: {client_name}")
            return

        from documentai_api.schemas.api_key import ApiKeyRecord

        for record in active_keys:
            deactivate_api_key(record[ApiKeyRecord.KEY_HASH])

        typer.echo(f"Deactivated {len(active_keys)} key(s) for client: {client_name}")


@app.command(name="list")
def list_keys(
    client_name: Annotated[str | None, typer.Option(help="Filter by client name")] = None,
    include_inactive: Annotated[
        bool, typer.Option("--include-inactive", help="Include inactive keys")
    ] = False,
) -> None:
    """List API keys, optionally filtered by client. Active keys only by default."""
    from documentai_api.schemas.api_key import ApiKeyRecord
    from documentai_api.services import ddb as ddb_service
    from documentai_api.utils.auth import get_active_keys_for_client
    from documentai_api.utils.env import API_KEYS_TABLE_NAME, get_required_env

    try:
        if client_name and not include_inactive:
            records = get_active_keys_for_client(client_name)
        else:
            table_name = get_required_env(API_KEYS_TABLE_NAME)
            all_records = ddb_service.scan(table_name)
            if client_name:
                all_records = [
                    r for r in all_records if r.get(ApiKeyRecord.CLIENT_NAME) == client_name
                ]
            if not include_inactive:
                all_records = [r for r in all_records if r.get(ApiKeyRecord.IS_ACTIVE, False)]
            records = all_records
    except Exception as e:
        typer.echo(f"Error: Failed to list keys: {e}", err=True)
        raise typer.Exit(code=1) from None

    if not records:
        typer.echo("No keys found.")
        return

    typer.echo("")
    typer.echo(f"{'CLIENT':<30} {'ACTIVE':<8} {'CREATED':<30} {'EXPIRES'}")
    typer.echo("-" * 90)
    for record in records:
        client = record.get(ApiKeyRecord.CLIENT_NAME, "unknown")
        active = str(record.get(ApiKeyRecord.IS_ACTIVE, False))
        created = record.get(ApiKeyRecord.CREATED_AT, "unknown")
        expires = record.get(ApiKeyRecord.EXPIRES_AT, "never")
        typer.echo(f"{client:<30} {active:<8} {created:<30} {expires}")
    typer.echo("")


if __name__ == "__main__":
    app()
