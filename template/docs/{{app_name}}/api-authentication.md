# API Authentication

The DocumentAI API currently supports a single shared API key for authentication.

## For API Users

### Getting Your API Key

**If you have AWS access:**
```bash
aws ssm get-parameter \
  --name "/{app_name}-{env}/api-auth-insecure-shared-key" \
  --with-decryption \
  --query "Parameter.Value" \
  --output text
 ```

**If you do not have AWS access:**

Contact your system administrator to obtain an API Key for your environment.

### Making Authenticated Requests

Include the API key in the `API-Key` header with every request:

**Example with curl:**
```bash
curl -H "API-Key: your-api-key-here" \
     -F "file=@document.pdf" \
     https://documentai.example.com/v1/documents
```

**Example with Python**:

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
**422 Unprocessable Entity** - Missing API-Key header

```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["header", "API-Key"],
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
The DocumentAI API uses the value from `API_AUTH_INSECURE_SHARED_KEY` env var to compare against the `API-Key` header in requests.

### Storing the Key

**Local Development**:
```bash
export API_AUTH_INSECURE_SHARED_KEY="your-generated-api-key"
```

**Hosted Environments**:

Store the key securely at rest and inject the env var into the API server environment. If you are using template-infra, add this to your app config:

```hcl
API_AUTH_TOKEN = {
  manage_method     = "manual"
  secret_store_name = "/${var.app_name}-${var.environment}/api-auth-insecure-shared-key"
}
```

### Rotating the Key
1. Update the value in your secret store
2. Restart/redeploy the server to pick up the new value


### Security Considerations
**This is a skeleton key implementation** - all users share the same API key. This is suitable for:

- Demo environments
- Internal tools
- Development/staging environments

**Not suitable for**:

- Production systems with multiple users
- Systems requiring user-specific permissions
- Compliance-sensitive applications