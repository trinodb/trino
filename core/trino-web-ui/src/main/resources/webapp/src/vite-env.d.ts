/// <reference types="vite/client" />

// The deep monaco-editor ESM entry points used by src/monaco.ts ship no declaration
// files; the editor core re-exports the same API as the typed root entry point.
declare module 'monaco-editor/esm/vs/editor/edcore.main.js' {
    export * from 'monaco-editor'
}
declare module 'monaco-editor/esm/vs/basic-languages/*'
declare module 'monaco-editor/esm/vs/language/json/monaco.contribution.js'
