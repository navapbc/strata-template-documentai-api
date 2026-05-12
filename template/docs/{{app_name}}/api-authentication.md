# API Authentication

The DocumentAI API uses API key authentication. Keys are validated against a DynamoDB table, with in-memory caching to minimize latency.

For local development, a single shared key can be used instead (see [Local Development](#local-development)).

## For API Users

### Getting Your API Key

Contact your system administrator to obtain an API key for your environment. You will receive a key in the format:

```
docai_<random>
```

Store it securely — it is shown only once at generation time.

### Making Authenticated Requests

Include the API key in the `API-Key` header with every request:

**Example with curl:**
```bash
curl -H "API-Key: your-api-key-here" \
     -F "file=@document.pdf" \
     https://documentai.example.com/v1/documents
```

**Example with Python:**
```python
import requests

headers = {"API-Key": "your-api-key-here"}
files = {"file": open("document.pdf", "rb")}

response = requests.post(
    "https://documentai.example.com/v1/documents",
    headers=headers,
    files=files
)
print(response.json())
```

### Endpoint Authentication

Visit `/docs` to view all available endpoints.

Protected routes are indicated by the lock icon (🔒). Public routes (e.g., `/health`) do not require authentication.

### Error Responses

**401 Unauthorized** — Invalid or missing API key:
```json
{
  "detail": "Invalid API key"
}
```

## For Maintainers

### How It Works

When `API_AUTH_ENABLED=true`, the API validates keys against a DynamoDB table (`api-keys`). On each request:

1. The presented key is hashed with SHA-256
2. The hash is looked up in the `api-keys` DynamoDB table (with a 5-minute in-memory cache)
3. The record is checked for `isActive=true` and an optional `expiresAt` date
4. If valid, the request proceeds; otherwise a 401 is returned

Keys are never stored or logged in plaintext — only the SHA-256 hash is persisted.

### Required Environment Variables

| Variable | Description |
|---|---|
| `API_AUTH_ENABLED` | Set to `true` to enable DynamoDB-based auth (default: `false`) |
| `API_KEYS_TABLE_NAME` | Name of the DynamoDB api-keys table |
| `API_AUTH_CACHE_TTL` | Cache TTL in seconds (default: `300`) |

### Generating an API Key

Use the `api-keys generate` command. The plaintext key is displayed once and not stored — share it securely with the client.

```bash
api-keys generate --client-name "my-service" --environment prod
```

With optional expiry:
```bash
api-keys generate \
  --client-name "my-service" \
  --environment prod \
  --expires-at "2027-01-01T00:00:00+00:00"
```

Output:
```
API Key (save this — it will not be shown again):
  docai_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6

Client:      my-service
Environment: prod
Expires:     never
```

### Listing Keys

```bash
# all active keys
api-keys list

# filter by client
api-keys list --client-name "my-service"

# include inactive keys
api-keys list --include-inactive
api-keys list --client-name "my-service" --include-inactive
```

### Revoking a Key

```bash
# deactivate a specific key by plaintext key
api-keys deactivate --client-name "my-service" --api-key docai_a1b2c3...

# deactivate all active keys for a client
api-keys deactivate --client-name "my-service" --all
```

Deactivation takes effect within one cache TTL period (default 5 minutes).

### Rotating a Key

1. Generate a new key with `api-keys generate`
2. Share the new key with the client
3. Once the client confirms they have migrated, run `api-keys deactivate --client-name "my-service" --all`

### Local Development

For local development, `API_AUTH_ENABLED` defaults to `false` and the API falls back to a single shared key via `API_AUTH_INSECURE_SHARED_KEY`.

A default key is preconfigured in `local.env.example`. Copy it to `.env` to get started:

```bash
cp local.env.example .env
```

**This single-key mode is not suitable for production.** Enable `API_AUTH_ENABLED=true` with the DynamoDB table for all hosted environments.

## Security Considerations

- Keys are hashed with SHA-256 before storage — the plaintext key cannot be recovered from DynamoDB
- HTTPS is enforced by the load balancer — keys are never transmitted in plaintext
- Keys are never logged
- Deactivation takes effect immediately (cache is invalidated on deactivate)
- Auth failures are logged with client name (where available) but never the key itself

## Known Limitations

The following are known gaps in the v0.1 implementation. They are suitable for internal service-to-service use with a small number of known calling systems, but should be addressed before broader rollout.

**Rate limiting**
No rate limiting is implemented at the application layer. Brute-force or abuse protection should be configured at the API Gateway or ALB level using WAF rules.

**Key rotation policy**
Keys do not expire automatically unless `expiresAt` is set at generation time. Rotation is a manual process — generate a new key, migrate the client, then deactivate the old one. Future versions could enforce a maximum key lifetime.

**HTTPS enforcement**
HTTPS is not enforced at the application layer — it is the responsibility of the load balancer. Direct connections to the application that bypass the ALB would transmit keys in plaintext. Ensure the application is never exposed directly.
