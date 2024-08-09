# Preview Web UI

The Preview Web UI for Trino is a prospective project to create a new web
interface for Trino. This initiative aims to enhance user interaction and
experience by providing  a modern and intuitive web interface Trino.

For further details, the original issue and discussions can be found at
https://github.com/trinodb/trino/issues/22697.

We're expecting multiple PRs to get merged into the Preview Web UI and will
swap the old UI to the new one at the right time, making it visible to the
end users once all functionalities have been migrated.

## Building the Preview Web UI

Preview Web UI is a potential new web interface featuring modernized tools
and an updated look and feel. It is disabled by default and enabled only
in the full development server by setting the `web-ui.preview.enabled=true`
property.

1. Run the `PreviewUiQueryRunner` class. This will start a minimalistic
   development server configured with the Preview UI.

2. Install dependencies:

```
$ cd core/trino-web-ui/src/main/resources/webapp-preview
$ npm install
```

3. Run the frontend in development mode. This will enable Hot Module Replacement,
   providing instant and accurate updates without reloading the page or losing the
   application state.

```
$ npm run dev

VITE v5.3.4  ready in 108 ms

➜  Local:   http://localhost:5173/ui/preview
➜  Network: use --host to expose
➜  press h + enter to show help
```
