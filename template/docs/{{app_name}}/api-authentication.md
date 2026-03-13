# API Authentication

The DocumentAI API uses a simple token-based authentication system for securing endpoints.

## For API Users

### Getting Your API Token

**If you have AWS access:**
```bash
aws ssm get-parameter \
  --name "/app-docai-{env}/api-auth-token" \
  --with-decryption \
  --query "Parameter.Value" \
  --output text
 ```

**If you do not have AWS access:**

Contact your system administrator to obtain an API token for your environment.

### Making Authenticated Requests

Include the API token in the `X-API-Key` header with every request:

**Example with curl:**
```bash
curl -H "X-API-Key: your-token-here" \
     -F "file=@document.pdf" \
     https://documentai.example.com/v1/documents
```

**Example with Python**:

```python
import requests

headers = {"X-API-Key": "your-token-here"}
files = {"file": open("document.pdf", "rb")}

response = requests.post(
    "https://documentai.example.com/v1/documents",
    headers=headers,
    files=files
)
print(response.json())
```

### Public Endpoints (No Auth Required)
- `GET /` - Root/status
- `GET /health` - Health check
- `GET /config` - API configuration

### Protected Endpoints (Auth Required)
All other endpoints require authentication:

- `POST /v1/documents` - Upload document
- `GET /v1/documents/{job_id}` - Get job status
- `GET /v1/schemas` - List schemas
- `GET /v1/schemas/{document_type}` - Get schema details


### Error Responses
**422 Unprocessable Entity** - Missing X-API-Key header

```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["header", "X-API-Key"],
      "msg": "Field required"
    }
  ]
}
```

**401 Unauthorized** - Invalid API Key

```json
{
  "detail": "Invalid API key"
}
```

**500 Internal Server Error** - API key not configured (contact administrator)

```json
{
  "detail": "API key not configured"
}
```

## For Maintainers
Generating the API Token
Generate a secure random token:

```bash
# Generate a 32-character random token
openssl rand -hex 32
```

### Storing the Token

**Local Development**:
```bash
export API_AUTH_INSECURE_SHARED_KEY="your-generated-token"
```

**AWS Environments (Recommended)**:

1. Store token in AWS Systems Manager Parameter Store:

```bash
aws ssm put-parameter \
  --name "/app-docai-{env}/api-auth-token" \
  --value "your-generated-token" \
  --type "SecureString" \
  --description "API authentication token for DocumentAI"
```

**Note:** The initial API token is created as part of the infrastructure


### Rotating the Token
1. Generate a new token (see above)

2. Update the SSM parameter:
```
aws ssm put-parameter \
  --name "/app-docai-{env}/api-auth-token" \
  --value "new-generated-token" \
  --type "SecureString" \
  --overwrite
```


**Note**: The token is cached for 60 minutes. After rotation, the old token will continue to work for up to 60 minutes until the cache expires.

### How It Works
- **Local Dev**: `API_AUTH_TOKEN` contains the actual token value
- **AWS**: `API_AUTH_TOKEN` contains an SSM parameter ARN
- The app detects ARN format and fetches the token from SSM
- Tokens are cached for 60 minutes to reduce SSM API calls

### Security Considerations
**This is a skeleton key implementation** - all users share the same token. This is suitable for:

- Demo environments
- Internal tools
- Development/staging environments

**Not suitable for**:

- Production systems with multiple users
- Systems requiring user-specific permissions
- Compliance-sensitive applications