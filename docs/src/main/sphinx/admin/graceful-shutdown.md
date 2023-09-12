# Graceful shutdown

Trino has a graceful shutdown API that can be used exclusively on workers in
order to ensure that they terminate without affecting running queries, given a
sufficient grace period.

You can invoke the API with a HTTP PUT request:

```bash
curl -v -X PUT -d '"SHUTTING_DOWN"' -H "Content-type: application/json" \
    http://worker:8081/v1/info/state
```

A successful invocation is logged with a `Shutdown requested` message at
`INFO` level in the worker server log.

Keep the following aspects in mind:

- If your cluster is secure, you need to provide a basic-authorization header,
  or satisfy whatever other security you have enabled.
- If you have TLS/HTTPS enabled, you have to ensure the worker certificate is
  CA signed, or trusted by the server calling the shut down endpoint.
  Otherwise, you can make the call `--insecure`, but that isn't recommended.
- The `default` {doc}`/security/built-in-system-access-control` does not allow
  graceful shutdowns. You can use the `allow-all` system access control, or
  configure {ref}`system information rules
  <system-file-auth-system-information>` with the `file` system access
  control. These configuration must be present on all workers.

## Shutdown behavior

Once the API is called, the worker performs the following steps:

- Go into `SHUTTING_DOWN` state.
- Sleep for `shutdown.grace-period`, which defaults to 2 minutes.
  : - After this, the coordinator is aware of the shutdown and stops sending
      tasks to the worker.
- Block until all active tasks are complete.
- Sleep for the grace period again in order to ensure the coordinator sees
  all tasks are complete.
- Shutdown the application.
