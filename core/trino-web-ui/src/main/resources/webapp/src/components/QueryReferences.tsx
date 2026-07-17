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
import {
    Alert,
    Box,
    CircularProgress,
    Divider,
    Grid,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableRow,
    Typography,
} from '@mui/material'
import { QueryRoutine, QueryTable } from '../api/webapp/api.ts'
import { QueryProgressBar } from './QueryProgressBar.tsx'
import { useQueryStatus } from './QueryStatusContext'

export const QueryReferences = () => {
    const { queryStatusInfo, loading, error, ended } = useQueryStatus()

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
            {error && <Alert severity="error">{error}</Alert>}

            {!loading && !error && queryStatusInfo && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <Box sx={{ width: '100%' }}>
                                <QueryProgressBar queryInfoBase={queryStatusInfo} />
                            </Box>

                            {ended ? (
                                <Grid container spacing={3}>
                                    <Grid size={{ xs: 12, md: 12 }}>
                                        <Box sx={{ pt: 2 }}>
                                            <Typography variant="h6">Referenced Tables</Typography>
                                            <Divider />
                                        </Box>
                                        {renderReferencedTables(queryStatusInfo.referencedTables)}
                                        <Box sx={{ pt: 2 }}>
                                            <Typography variant="h6">Routines</Typography>
                                            <Divider />
                                        </Box>
                                        {renderRoutines(queryStatusInfo.routines)}
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
