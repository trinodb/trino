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
import { LightAsync as SyntaxHighlighter } from 'react-syntax-highlighter'
import { a11yLight, a11yDark } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { Theme as ThemeStore, useConfigStore } from '../store'

export interface ICodeBlockProps {
    code: string
    language: string
    height?: string
    noBottomBorder?: boolean
}

export const CodeBlock = (props: ICodeBlockProps) => {
    const config = useConfigStore()
    const { code, language, height, noBottomBorder } = props
    const prefersDarkMode = useMediaQuery('(prefers-color-scheme: dark)')

    const styleToUse = () => {
        if (config.theme === ThemeStore.Auto) {
            return prefersDarkMode ? a11yDark : a11yLight
        } else if (config.theme === ThemeStore.Dark) {
            return a11yDark
        } else {
            return a11yLight
        }
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
                height: {
                    xs: '100%',
                    lg: height,
                },
            })}
        >
            <SyntaxHighlighter
                language={language}
                style={styleToUse()}
                customStyle={{
                    padding: '4px',
                    margin: 0,
                    borderRadius: 0,
                    height: '100%',
                    overflow: 'auto',
                }}
                wrapLongLines
            >
                {code}
            </SyntaxHighlighter>
        </Box>
    )
}
