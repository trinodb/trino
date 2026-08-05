# Web UI

This directory contains the default Trino Web UI. The legacy Web UI is served
from `webapp-legacy` and remains available at `/ui/legacy`.

## Building the Web UI

1. Run the `WebUiQueryRunner` class. This starts a minimal development server
   configured with the Web UI.

2. Install dependencies:

```
$ cd core/trino-web-ui/src/main/resources/webapp
$ bun install
```

3. Run the frontend in development mode. This enables Hot Module Replacement,
   providing instant updates without reloading the page or losing application
   state.

```
$ bun run dev

VITE v7.1.7  ready in 100 ms

➜  Local:   http://localhost:5173/ui
➜  Network: use --host to expose
➜  press h + enter to show help
```
