# trino-opa

This plugin enables Trino to use [Open Policy Agent
(OPA)](https://www.openpolicyagent.org/) as an authorization engine.

## Configuration

The OPA plugin is configured through properties in the catalog configuration file (e.g., `etc/catalogs/opa.properties`).

### Core Properties

| Property | Default | Description |
|----------|---------|-------------|
| `opa.policy-fetcher-uri` | | URI to the OPA server |
| `opa.authorization-policy-name` | | The OPA policy package to query for authorization decisions |

### HTTP Request Headers Configuration

The plugin can optionally pass HTTP request headers from the client to OPA policies, enabling fine-grained authorization decisions based on request context.

| Property | Default | Description |
|----------|---------|-------------|
| `opa.include-request-headers` | `false` | Enable extraction and passing of HTTP headers to OPA |
| `opa.additional-headers` | (empty) | Comma-separated list of HTTP header names to include in OPA requests |

## Usage Examples

### Basic Configuration

```properties
# opa.properties
opa.policy-fetcher-uri=http://localhost:8181
opa.authorization-policy-name=trino/authz
opa.include-request-headers=true
opa.additional-headers=Authorization,X-Tenant-Id,X-Request-Id,X-Forwarded-For
```

### Use Cases

#### 1. Tenant Isolation

Enforce query access based on tenant information in request headers:

```properties
opa.additional-headers=X-Tenant-Id
```

OPA receives:
```json
{
  "context": {
    "identity": { "user": "alice" },
    "requestHeaders": {
      "X-Tenant-Id": ["acme-corp"]
    }
  }
}
```

OPA policy (Rego):
```rego
default allow = false

allow {
  input.context.identity.user == "alice"
  input.context.requestHeaders["X-Tenant-Id"][0] == "acme-corp"
}
```

#### 2. Proxy Chain Validation

Validate requests coming through proxy chains using X-Forwarded-For:

```properties
opa.additional-headers=X-Forwarded-For
```

OPA policy validates that the original client IP is from an allowed range.

#### 3. Custom Authentication

Use custom authentication headers (beyond standard Authorization):

```properties
opa.additional-headers=Authorization,X-API-Key,X-Custom-Auth
```

#### 4. Request Correlation and Audit

Track requests through logs using correlation IDs:

```properties
opa.additional-headers=X-Request-Id,X-Correlation-Id
```

## Security Considerations

- **Whitelist Approach**: Only headers explicitly listed in `opa.additional-headers` are passed to OPA. This provides a secure, deny-by-default model.
- **Header Validation**: OPA policies should validate header values. The plugin passes headers as-is from clients.
- **Sensitive Data**: Do not include sensitive headers (cookies, tokens) unless OPA policies have appropriate handling.
- **Multi-valued Headers**: Headers can have multiple values (e.g., X-Forwarded-For with proxy chains, Set-Cookie). OPA receives all values as a list.
