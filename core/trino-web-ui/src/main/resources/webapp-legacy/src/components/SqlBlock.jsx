import React, { useEffect, useRef } from 'react'
import hljs from 'highlight.js/lib/core'
import sql from 'highlight.js/lib/languages/sql'
hljs.registerLanguage('sql', sql)

export const SqlBlock = ({ language, code }) => {
    const codeRef = useRef()
    useEffect(() => {
        if (codeRef && codeRef.current) {
            hljs.highlightElement(codeRef.current)
        }
    }, [code])

    return (
        <code className="language-sql sql" ref={codeRef}>
            {code}
        </code>
    )
}
