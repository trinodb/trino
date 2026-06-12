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
import React, { useMemo } from 'react'
import { Box, useTheme } from '@mui/material'

export interface CodePreviewProps {
    code: string
    height?: string
}

// SQL keywords recognized by Trino — kept intentionally short for preview snippets.
// Matching is case-insensitive via the regex flag.
const SQL_KEYWORDS = new Set([
    'SELECT',
    'FROM',
    'WHERE',
    'AND',
    'OR',
    'NOT',
    'IN',
    'AS',
    'ON',
    'JOIN',
    'LEFT',
    'RIGHT',
    'INNER',
    'OUTER',
    'FULL',
    'CROSS',
    'GROUP',
    'BY',
    'ORDER',
    'HAVING',
    'LIMIT',
    'OFFSET',
    'UNION',
    'ALL',
    'INSERT',
    'INTO',
    'VALUES',
    'UPDATE',
    'SET',
    'DELETE',
    'CREATE',
    'DROP',
    'ALTER',
    'TABLE',
    'VIEW',
    'INDEX',
    'WITH',
    'RECURSIVE',
    'CASE',
    'WHEN',
    'THEN',
    'ELSE',
    'END',
    'EXISTS',
    'BETWEEN',
    'LIKE',
    'IS',
    'NULL',
    'TRUE',
    'FALSE',
    'DISTINCT',
    'ASC',
    'DESC',
    'CAST',
    'IF',
    'OVER',
    'PARTITION',
    'ROWS',
    'RANGE',
    'UNBOUNDED',
    'PRECEDING',
    'FOLLOWING',
    'CURRENT',
    'ROW',
    'FETCH',
    'FIRST',
    'NEXT',
    'ONLY',
    'EXCEPT',
    'INTERSECT',
    'LATERAL',
    'UNNEST',
    'TABLESAMPLE',
    'EXPLAIN',
    'ANALYZE',
    'FORMAT',
    'SHOW',
    'DESCRIBE',
    'USE',
    'SCHEMA',
    'CATALOG',
    'COLUMNS',
    'TABLES',
    'SCHEMAS',
    'CATALOGS',
    'FUNCTIONS',
    'SESSION',
    'PREPARE',
    'EXECUTE',
    'DEALLOCATE',
    'COMMIT',
    'ROLLBACK',
    'GRANT',
    'REVOKE',
    'DENY',
    'CALL',
    'REFRESH',
    'MATERIALIZED',
])

// Tokenize SQL into typed spans for syntax highlighting.
// Handles: single-quoted strings, single-line comments (--), block comments,
// numbers, keywords, and plain identifiers/operators.
const TOKEN_REGEX = /('(?:[^'\\]|\\.)*'|--[^\n]*|\/\*[\s\S]*?\*\/|\b\d+(?:\.\d+)?\b|\b[a-zA-Z_]\w*\b|[^\s])/g

interface Token {
    text: string
    type: 'keyword' | 'string' | 'comment' | 'number' | 'plain'
}

function tokenizeSql(code: string): Token[] {
    const tokens: Token[] = []
    let lastIndex = 0

    for (const match of code.matchAll(TOKEN_REGEX)) {
        const matchIndex = match.index!
        // Preserve whitespace between tokens
        if (matchIndex > lastIndex) {
            tokens.push({ text: code.slice(lastIndex, matchIndex), type: 'plain' })
        }

        const text = match[0]
        let type: Token['type'] = 'plain'

        if (text.startsWith("'") || text.startsWith('"')) {
            type = 'string'
        } else if (text.startsWith('--') || text.startsWith('/*')) {
            type = 'comment'
        } else if (/^\d/.test(text)) {
            type = 'number'
        } else if (SQL_KEYWORDS.has(text.toUpperCase())) {
            type = 'keyword'
        }

        tokens.push({ text, type })
        lastIndex = matchIndex + text.length
    }

    // Trailing whitespace
    if (lastIndex < code.length) {
        tokens.push({ text: code.slice(lastIndex), type: 'plain' })
    }

    return tokens
}

// Colors aligned with Monaco's default VS / VS Dark themes
const LIGHT_COLORS: Record<Token['type'], string> = {
    keyword: '#0000ff',
    string: '#a31515',
    comment: '#008000',
    number: '#098658',
    plain: '#000000',
}

const DARK_COLORS: Record<Token['type'], string> = {
    keyword: '#569cd6',
    string: '#ce9178',
    comment: '#6a9955',
    number: '#b5cea8',
    plain: '#d4d4d4',
}

export const CodePreview = React.memo(function CodePreview(props: CodePreviewProps) {
    const { code, height } = props
    const theme = useTheme()
    const isDark = theme.palette.mode === 'dark'
    const colors = isDark ? DARK_COLORS : LIGHT_COLORS

    const highlighted = useMemo(() => {
        const tokens = tokenizeSql(code)
        return tokens.map((token, i) => (
            <span key={i} style={{ color: colors[token.type] }}>
                {token.text}
            </span>
        ))
    }, [code, colors])

    return (
        <Box
            component="pre"
            sx={{
                margin: 0,
                padding: 1,
                fontFamily: '"Droid Sans Mono", "monospace", monospace',
                fontSize: '13px',
                lineHeight: '18px',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-all',
                overflow: 'auto',
                width: '100%',
                height: height,
                border: `1px solid ${isDark ? '#3f3f3f' : '#ddd'}`,
                borderBottom: 'none',
                backgroundColor: isDark ? '#1e1e1e' : '#fffffe',
                color: isDark ? '#d4d4d4' : '#000000',
            }}
        >
            {highlighted}
        </Box>
    )
})
