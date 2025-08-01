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
import { useParams } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { Alert, Box, CircularProgress, Grid2 as Grid } from '@mui/material'
import { Texts } from '../constant.ts'
import { queryStatusApi, QueryStatusInfo } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import { CodeBlock } from './CodeBlock.tsx'

export const QueryJson = () => {
    const { queryId } = useParams()

    const [queryJson, setQueryJson] = useState<string | null>(null)
    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        if (queryId) {
            getQueryJson()
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [queryId])

    const getQueryJson = () => {
        if (queryId) {
            queryStatusApi(queryId).then((apiResponse: ApiResponse<QueryStatusInfo>) => {
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    setQueryJson(JSON.stringify(apiResponse.data, null, 2))
                    setError(null)
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
        }
    }

    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{Texts.Error.QueryNotFound}</Alert>}

            {!loading && !error && queryJson && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <CodeBlock language="json" code={queryJson} />
                        </Box>
                    </Grid>
                </Grid>
            )}
        </>
    )
}
