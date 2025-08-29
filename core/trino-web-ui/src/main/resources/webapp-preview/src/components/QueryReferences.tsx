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
import { useEffect, useRef, useState } from 'react'
import {
    Alert,
    Box,
    CircularProgress,
    Divider,
    Grid2 as Grid,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableRow,
    Typography,
} from '@mui/material'
import { queryStatusApi, QueryRoutine, QueryStatusInfo, QueryTable } from '../api/webapp/api.ts'
import { Texts } from '../constant.ts'
import { ApiResponse } from '../api/base.ts'
import { QueryProgressBar } from './QueryProgressBar.tsx'

interface IQueryStatus {
    info: QueryStatusInfo | null
    ended: boolean
}

export const QueryReferences = () => {
    const { queryId } = useParams()
    const initialQueryStatus: IQueryStatus = {
        info: null,
        ended: false,
    }

    const [queryStatus, setQueryStatus] = useState<IQueryStatus>(initialQueryStatus)

    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)
    const queryStatusRef = useRef(queryStatus)

    useEffect(() => {
        queryStatusRef.current = queryStatus
    }, [queryStatus])

    useEffect(() => {
        const runLoop = () => {
            const queryEnded = !!queryStatusRef.current.info?.finalQueryInfo
            if (!queryEnded) {
                getQueryStatus()
                setTimeout(runLoop, 3000)
            }
        }

        if (queryId) {
            queryStatusRef.current = initialQueryStatus
        }

        runLoop()
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [queryId])

    const getQueryStatus = () => {
        if (queryId) {
            queryStatusApi(queryId, false).then((apiResponse: ApiResponse<QueryStatusInfo>) => {
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    setQueryStatus({
                        info: apiResponse.data,
                        ended: apiResponse.data.finalQueryInfo,
                    })
                    setError(null)
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
        }
    }

    const renderReferencedTables = (tables: QueryTable[]) => {
        if (!tables || tables.length === 0) {
            return (
                <Box sx={{ width: '100%', mt: 1 }}>
                    <Alert severity="info">No referenced tables.</Alert>
                </Box>
            )
        }

        return (
            <TableContainer>
                <Table aria-label="simple table">
                    <TableBody>
                        {tables.map((table: QueryTable) => {
                            const tableName = `${table.catalog}.${table.schema}.${table.table}`
                            return (
                                <TableRow key={tableName}>
                                    <TableCell sx={{ width: '50%' }}>{tableName}</TableCell>
                                    <TableCell sx={{ width: '50%' }}>
                                        {`Authorization: ${table.authorization}, Directly Referenced: ${table.directlyReferenced}`}
                                    </TableCell>
                                </TableRow>
                            )
                        })}
                    </TableBody>
                </Table>
            </TableContainer>
        )
    }

    const renderRoutines = (routines: QueryRoutine[]) => {
        if (!routines || routines.length === 0) {
            return (
                <Box sx={{ width: '100%', mt: 1 }}>
                    <Alert severity="info">No referenced routines.</Alert>
                </Box>
            )
        }

        return (
            <TableContainer>
                <Table aria-label="simple table">
                    <TableBody>
                        {routines.map((routine: QueryRoutine, idx: number) => (
                            <TableRow key={`${routine.routine}-${idx}`}>
                                <TableCell sx={{ width: '50%' }}>{`${routine.routine}`}</TableCell>
                                <TableCell sx={{ width: '50%' }}>{`Authorization: ${routine.authorization}`}</TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
        )
    }

    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{Texts.Error.QueryNotFound}</Alert>}

            {!loading && !error && queryStatus.info && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <Box sx={{ width: '100%' }}>
                                <QueryProgressBar queryInfoBase={queryStatus.info} />
                            </Box>

                            {queryStatus.ended ? (
                                <Grid container spacing={3}>
                                    <Grid size={{ xs: 12, md: 12 }}>
                                        <Box sx={{ pt: 2 }}>
                                            <Typography variant="h6">Referenced Tables</Typography>
                                            <Divider />
                                        </Box>
                                        {renderReferencedTables(queryStatus.info.referencedTables)}
                                        <Box sx={{ pt: 2 }}>
                                            <Typography variant="h6">Routines</Typography>
                                            <Divider />
                                        </Box>
                                        {renderRoutines(queryStatus.info.routines)}
                                    </Grid>
                                </Grid>
                            ) : (
                                <>
                                    <Box sx={{ width: '100%', mt: 1 }}>
                                        <Alert severity="info">
                                            References will appear automatically when query completes.
                                        </Alert>
                                    </Box>
                                </>
                            )}
                        </Box>
                    </Grid>
                </Grid>
            )}
        </>
    )
}
