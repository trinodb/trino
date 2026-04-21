/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { useState } from 'react'
import { Box, useMediaQuery } from '@mui/material'
import Editor, { type OnMount } from '@monaco-editor/react'
import merge from 'lodash.merge'
import { Theme as ThemeStore, useConfigStore } from '../store'
import type { editor as MonacoEditor } from 'monaco-editor'

const FALLBACK_LINE_HEIGHT_PX = 20
const EDITOR_EXTRA_PADDING = 4
const VIEWPORT_HEIGHT_RATIO = 0.75
const LINE_BREAK_REGEX = /\r\n|\r|\n/

export interface ICodeBlockProps {
    code: string
    language: string
    height?: string
    noBottomBorder?: boolean
    monacoOptions?: MonacoEditor.IStandaloneEditorConstructionOptions
}

export const CodeBlock = (props: ICodeBlockProps) => {
    const config = useConfigStore()
    const { code, language, height, noBottomBorder, monacoOptions } = props
    const prefersDarkMode = useMediaQuery('(prefers-color-scheme: dark)')
    const [resolvedHeight, setResolvedHeight] = useState<string | undefined>(height)

    const themeToUse = () => {
        if (config.theme === ThemeStore.Auto) {
            return prefersDarkMode ? 'vs-dark' : 'light'
        } else if (config.theme === ThemeStore.Dark) {
            return 'vs-dark'
        } else {
            return 'light'
        }
    }

    // Calculates a dynamic editor height if none is provided.
    // Uses the editor's lineHeight and code line count to estimate total height,
    // adds extra padding, then clamps it so it never exceeds a fraction of the viewport.
    // Falls back to a fixed height if one was passed in.
    const handleEditorMount: OnMount = (editor, monaco) => {
        if (height) {
            setResolvedHeight(height)
            return
        }

        const estimateHeightFromLines = () => {
            const lineHeightOption = editor.getOption(monaco.editor.EditorOption.lineHeight)
            const lineHeight = typeof lineHeightOption === 'number' ? lineHeightOption : FALLBACK_LINE_HEIGHT_PX
            const lineCount = Math.max(code.split(LINE_BREAK_REGEX).length, 1)
            return lineCount * lineHeight + EDITOR_EXTRA_PADDING
        }

        const updateHeight = (contentHeight?: number) => {
            const rawHeight =
                typeof contentHeight === 'number' ? contentHeight + EDITOR_EXTRA_PADDING : estimateHeightFromLines()
            const viewportCap = window.innerHeight * VIEWPORT_HEIGHT_RATIO
            const clampedHeight = Math.min(rawHeight, viewportCap)
            setResolvedHeight(`${Math.round(clampedHeight)}px`)
        }

        updateHeight(editor.getContentHeight())

        const disposable = editor.onDidContentSizeChange((event) => {
            updateHeight(event.contentHeight)
        })

        editor.onDidDispose(() => disposable.dispose())
    }

    const defaultMonacoOptions: MonacoEditor.IStandaloneEditorConstructionOptions = {
        readOnly: true,
        domReadOnly: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        lineNumbers: 'off',
        folding: false,
        contextmenu: false,
        renderLineHighlight: 'none',
        overviewRulerLanes: 0,
        wordWrap: 'on',
        rulers: [],
        guides: { indentation: false },
        stickyScroll: { enabled: false },
        scrollBeyondLastColumn: 0,
    }

    return (
        <Box
            display="flex"
            flexDirection="column"
            flexGrow={1}
            sx={(theme) => ({
                padding: 0,
                borderRadius: 0,
                border: `1px solid ${theme.palette.mode === 'dark' ? '#3f3f3f' : '#ddd'}`,
                borderBottom: noBottomBorder ? 'none' : '',
                width: '100%',
                height: '100%',
                maxHeight: '90vh',
            })}
        >
            <Editor
                height={resolvedHeight}
                language={language}
                theme={themeToUse()}
                onMount={handleEditorMount}
                value={code}
                options={merge({}, defaultMonacoOptions, monacoOptions)}
            />
        </Box>
    )
}
