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
import { Link as RouterLink } from 'react-router-dom'
import {
    Box,
    Divider,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Typography,
} from '@mui/material'
import { Worker, workerApi } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import { useEffect, useState } from 'react'
import { Texts } from '../constant.ts'
import { useSnackbar } from './SnackbarContext.ts'
import { styled } from '@mui/material/styles'

const StyledLink = styled(RouterLink)(({ theme }) => ({
    textDecoration: 'none',
    color: theme.palette.info.main,
    '&:hover': {
        textDecoration: 'underline',
    },
}))

export const WorkersList = () => {
    const { showSnackbar } = useSnackbar()
    const [workersList, setWorkersList] = useState<Worker[]>([])
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        const runLoop = () => {
            getWorkersList()
            setTimeout(runLoop, 1000)
        }
        runLoop()
    }, [])

    useEffect(() => {
        if (error) {
            showSnackbar(error, 'error')
        }
    }, [error, showSnackbar])

    const getWorkersList = () => {
        setError(null)
        workerApi().then((apiResponse: ApiResponse<Worker[]>) => {
            if (apiResponse.status === 200 && apiResponse.data) {
                if (apiResponse.data) {
                    const sortedWorkers: Worker[] = apiResponse.data.sort((workerA, workerB) =>
                        workerA.coordinator === workerB.coordinator
                            ? workerA.nodeId.localeCompare(workerB.nodeId)
                            : workerA.coordinator
                              ? -1
                              : 1
                    )
                    setWorkersList(sortedWorkers)
                }
            } else {
                setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
            }
        })
    }

    return (
        <>
            <Box sx={{ pb: 2 }}>
                <Typography variant="h4">Workers</Typography>
            </Box>
            <Box sx={{ pt: 2 }}>
                <Typography variant="h6">Overview</Typography>
                <Divider />
            </Box>
            <TableContainer>
                <Table sx={{ minWidth: 650 }} aria-label="simple table">
                    <TableHead>
                        <TableRow>
                            <TableCell>Node ID</TableCell>
                            <TableCell align="right">Node IP</TableCell>
                            <TableCell align="right">Node version</TableCell>
                            <TableCell align="right">Coordinator</TableCell>
                            <TableCell align="right">State</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {workersList.map((worker: Worker) => (
                            <TableRow key={worker.nodeId} sx={{ '&:last-child td, &:last-child th': { border: 0 } }}>
                                <TableCell component="th" scope="row">
                                    <StyledLink to={`/workers/${worker.nodeId}`}>{worker.nodeId}</StyledLink>
                                </TableCell>
                                <TableCell align="right">
                                    <StyledLink to={`/workers/${worker.nodeId}`}>{worker.nodeIp}</StyledLink>
                                </TableCell>
                                <TableCell align="right">{worker.nodeVersion}</TableCell>
                                <TableCell align="right">{String(worker.coordinator)}</TableCell>
                                <TableCell align="right">{worker.state}</TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
        </>
    )
}
