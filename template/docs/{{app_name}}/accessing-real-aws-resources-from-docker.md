# Accessing Real AWS Resources from Docker

To access real AWS resources during local development, mount your AWS credentials into the Docker container.

## Setup

Add the following to your `docker-compose.yml`:

```yaml
services:
  {{app_name}}:
    volumes:
      - ~/.aws:/root/.aws:ro
    environment:
      - AWS_PROFILE
```

This mounts your local AWS credentials directory as read-only and passes through your AWS_PROFILE environment variable.

## Usage

```bash
# Set your AWS profile
export AWS_PROFILE=your-profile-name

# Start the container
docker compose up
```

## Security Considerations

- The :ro flag mounts credentials as read-only for safety
- Never commit AWS credentials to version control
