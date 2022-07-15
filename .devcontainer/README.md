# Using this devcontainer
This devcontainer is meant to easily set up a Java 11 / Maven 3 environment that works on a FALCON laptop with WSL2 (v1.2+) installed. In order for it to "just work", it does make a few assumptions:
## SSH_AUTH_SOCK
The WSL2 v1.2+ installer maps the `gpg-agent` SSH Agent to `$HOME/.ssh/agent.sock`. That file is assumed to exist and be a valid SSH agent socket.
## Scratch space
This devcontainer will create a folder named `scratch/` in your `$HOME` directory, if one doesn't already exist. This folder can be used to store temporary / transient data and to share files with other devcontainers that also map the `scratch/` folder.
## Maven cache
This devcontainer is configured to share its Maven cache with the host (WSL2). That way, re-building the container won't inadvertently clear the cache.
