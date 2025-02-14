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
import { Box, useMediaQuery } from '@mui/material'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { materialLight, materialDark } from 'react-syntax-highlighter/dist/esm/styles/prism'
import { Theme as ThemeStore, useConfigStore } from '../store'

export interface ICodeBlockProps {
    code: string
    language: string
}

export const CodeBlock = (props: ICodeBlockProps) => {
    const config = useConfigStore()
    const { code, language } = props
    const prefersDarkMode = useMediaQuery('(prefers-color-scheme: dark)')

    const styleToUse = () => {
        if (config.theme === ThemeStore.Auto) {
            return prefersDarkMode ? materialDark : materialLight
        } else if (config.theme === ThemeStore.Dark) {
            return materialDark
        } else {
            return materialLight
        }
    }

    return (
        <Box
            sx={{
                padding: 0,
                borderRadius: 0,
                backgroundColor: '#f5f5f5',
                overflow: 'auto',
                maxHeight: '400px',
                border: '1px solid #ddd',
            }}
        >
            <SyntaxHighlighter
                language={language}
                style={styleToUse()}
                customStyle={{ padding: '4px', margin: 0, borderRadius: 0 }}
            >
                {code}
            </SyntaxHighlighter>
        </Box>
    )
}
