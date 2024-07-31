# Preview Web UI

In addition to the [](/admin/web-interface), Trino includes a preview version of
a new web interface. It changes look and feel, available features, and many
other aspects. In the future this new user interface will replace the existing
user interface.

:::{warning}

The Preview Web UI is not suitable for production usage, and only available for
testing and evaluation purposes. Feedback and assistance with development is
encouraged. Find collaborators and discussions in ongoing pull requests and the
[#web-ui channel](https://trinodb.slack.com/messages/CKCEWGYT0).

:::

## Activation

The Preview Web UI is not available by default, and must be enabled in
[](config-properties) with the following configuration:


```properties
web-ui.preview.enabled=true
```

## Access

Once activated, users can access the interface in the URL context `/ui/preview`
after successful login to the [](/admin/web-interface). For example, the full
URL on a locally running Trino installation or Trino docker container without
TLS configuration is [http://localhost:8080/ui/preview](http://localhost:8080/ui/preview).
