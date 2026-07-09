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
import { useMemo } from 'react'
import { Alert, Box, CircularProgress, Grid } from '@mui/material'
import { CodeBlock } from './CodeBlock.tsx'
import { useQueryStatus } from './QueryStatusContext'

export const QueryJson = () => {
    const { queryStatusInfo, loading, error } = useQueryStatus()

    const queryJson = useMemo(() => {
        return queryStatusInfo ? JSON.stringify(queryStatusInfo, null, 2) : null
    }, [queryStatusInfo])

    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{error}</Alert>}

            {!loading && !error && queryJson && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <CodeBlock
                                language="json"
                                code={queryJson}
                                monacoOptions={{
                                    lineNumbers: 'on',
                                    minimap: { enabled: true },
                                    guides: { indentation: true },
                                    folding: true,
                                    stickyScroll: { enabled: true },
                                }}
                            />
                        </Box>
                    </Grid>
                </Grid>
            )}
        </>
    )
}
