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
    Button,
    CircularProgress,
    Divider,
    FormControl,
    InputLabel,
    MenuItem,
    Select,
    SelectChangeEvent,
    Stack,
    Typography,
} from '@mui/material'
import { ReactNode, useMemo, useState } from 'react'
import { WorkerThreadInfo, workerThreadApi } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import { Texts } from '../constant.ts'
import { useSnackbar } from './SnackbarContext.ts'
import { CodePreview } from './CodePreview.tsx'
import { CopyButton } from './CopyButton.tsx'

const ALL_THREADS = 'All Threads'
const QUERY_THREADS = 'Running Queries'
const ALL_THREAD_STATE = 'ALL'
const THREAD_STATES = [ALL_THREAD_STATE, 'RUNNABLE', 'BLOCKED', 'WAITING', 'TIMED_WAITING', 'NEW', 'TERMINATED']
const QUERY_THREAD_REGEX = /([0-9])*_([0-9])*_([0-9])*_.*?\.([0-9])*\.([0-9])*-([0-9])*-([0-9])*/
const THREAD_GROUP_REGEX = /(.*?)-[0-9]+/

type WorkerThreadGroups = Record<string, WorkerThreadInfo[]>

interface WorkerThreadSnapshotProps {
    nodeId: string
}

export const WorkerThreadSnapshot = ({ nodeId }: WorkerThreadSnapshotProps) => {
    const { showSnackbar } = useSnackbar()
    const [threads, setThreads] = useState<WorkerThreadInfo[] | null>(null)
    const [snapshotTime, setSnapshotTime] = useState<Date | null>(null)
    const [threadLoading, setThreadLoading] = useState<boolean>(false)
    const [threadError, setThreadError] = useState<boolean>(false)
    const [selectedGroup, setSelectedGroup] = useState<string>(ALL_THREADS)
    const [selectedThreadState, setSelectedThreadState] = useState<string>(ALL_THREAD_STATE)

    const threadGroups = useMemo(() => processThreads(threads), [threads])
    const filteredThreads = filterThreads(threadGroups, selectedGroup, selectedThreadState)
    const smallFormControlSx = {
        fontSize: '0.8rem',
    }
    const smallDropdownMenuPropsSx = {
        slotProps: {
            paper: {
                sx: {
                    '& .MuiMenuItem-root': smallFormControlSx,
                },
            },
        },
    }

    function processThreads(workerThreads: WorkerThreadInfo[] | null): WorkerThreadGroups | null {
        if (!workerThreads) {
            return null
        }

        const result: WorkerThreadGroups = {
            [ALL_THREADS]: workerThreads,
        }

        for (const thread of workerThreads) {
            if (thread.name.match(QUERY_THREAD_REGEX)) {
                result[QUERY_THREADS] ??= []
                result[QUERY_THREADS].push(thread)
            }

            const match = THREAD_GROUP_REGEX.exec(thread.name)
            const threadGroup = match ? match[1] : thread.name
            result[threadGroup] ??= []
            result[threadGroup].push(thread)
        }

        return result
    }

    function filterThreads(
        workerThreadGroups: WorkerThreadGroups | null,
        group: string,
        state: string
    ): WorkerThreadInfo[] {
        if (!workerThreadGroups || !workerThreadGroups[group]) {
            return []
        }

        return workerThreadGroups[group].filter((thread) => thread.state === state || state === ALL_THREAD_STATE)
    }

    const captureSnapshot = () => {
        setThreadLoading(true)
        setThreadError(false)
        workerThreadApi(nodeId).then((apiResponse: ApiResponse<WorkerThreadInfo[]>) => {
            setThreadLoading(false)
            if (apiResponse.status === 200 && apiResponse.data) {
                setThreads(apiResponse.data)
                setSnapshotTime(new Date())
                setSelectedGroup(ALL_THREADS)
                setSelectedThreadState(ALL_THREAD_STATE)
            } else {
                setThreads(null)
                setSnapshotTime(null)
                setThreadError(true)
                showSnackbar(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`, 'error')
            }
        })
    }

    const formatThread = (thread: WorkerThreadInfo): string => {
        return `${formatThreadHeader(thread)}\n${formatThreadStackTrace(thread)}`
    }

    const formatThreads = (workerThreads: WorkerThreadInfo[]): string => {
        return workerThreads.map(formatThread).join('\n\n')
    }

    const formatThreadHeader = (thread: WorkerThreadInfo): string => {
        const lockOwner = thread.lockOwnerId == null ? '' : ` ${thread.lockOwnerId}`
        return `${thread.name} ${thread.state} #${thread.id}${lockOwner}`
    }

    const formatThreadStackTrace = (thread: WorkerThreadInfo): string => {
        return thread.stackTrace
            .map((stackLine) => `  at ${stackLine.className}.${stackLine.method}(${stackLine.file}:${stackLine.line})`)
            .join('\n')
    }

    const getEmptyThreadsMessage = (): string => {
        if (selectedThreadState === ALL_THREAD_STATE) {
            return `No threads in group '${selectedGroup}'`
        }
        if (selectedGroup === ALL_THREADS) {
            return `No threads with state ${selectedThreadState}`
        }
        return `No threads in group '${selectedGroup}' with state ${selectedThreadState}`
    }

    const renderThreadSnapshotContent = (): ReactNode => {
        if (threadLoading) {
            return <CircularProgress size={24} />
        }

        if (threadError) {
            return <Alert severity="error">Thread snapshot could not be loaded</Alert>
        }

        if (!threadGroups) {
            return (
                <Button variant="outlined" size="small" onClick={captureSnapshot}>
                    Capture Snapshot
                </Button>
            )
        }

        if (filteredThreads.length === 0) {
            return <Alert severity="info">{getEmptyThreadsMessage()}</Alert>
        }

        return (
            <Box
                sx={(theme) => ({
                    backgroundColor: theme.palette.mode === 'dark' ? '#1e1e1e' : '#fffffe',
                    border: `1px solid ${theme.palette.mode === 'dark' ? '#3f3f3f' : '#ddd'}`,
                    color: theme.palette.mode === 'dark' ? '#d4d4d4' : '#000000',
                })}
            >
                {filteredThreads.map((thread) => (
                    <Box key={thread.id} sx={{ pb: 1 }}>
                        <Box
                            sx={{
                                alignItems: 'center',
                                display: 'flex',
                                gap: 0.5,
                                minWidth: 0,
                                px: 1,
                                pt: 1,
                            }}
                        >
                            <Typography
                                component="span"
                                sx={{
                                    fontFamily: '"Droid Sans Mono", "monospace", monospace',
                                    fontSize: '13px',
                                    lineHeight: '18px',
                                    minWidth: 0,
                                    overflowWrap: 'anywhere',
                                }}
                            >
                                {formatThreadHeader(thread)}
                            </Typography>
                            <CopyButton
                                aria-label={`Copy stack trace for thread ${thread.id}`}
                                color="primary"
                                size="small"
                                sx={{ p: 0.25 }}
                                text={formatThread(thread)}
                                successMessage="Thread stack trace copied"
                                tooltip="Copy thread stack trace"
                            />
                        </Box>
                        <CodePreview
                            code={formatThreadStackTrace(thread)}
                            sx={{
                                border: 'none',
                                paddingTop: 0,
                            }}
                        />
                    </Box>
                ))}
            </Box>
        )
    }

    return (
        <>
            <Box sx={{ pt: 2 }}>
                <Stack
                    direction={{ xs: 'column', md: 'row' }}
                    spacing={2}
                    sx={{ alignItems: { xs: 'stretch', md: 'center' } }}
                >
                    <Box sx={{ display: 'flex', alignItems: 'center', flexGrow: 1 }}>
                        <Typography variant="h6">Thread Snapshot</Typography>
                        {threadGroups && (
                            <CopyButton
                                aria-label="Copy displayed stack traces"
                                color="primary"
                                disabled={filteredThreads.length === 0}
                                text={formatThreads(filteredThreads)}
                                successMessage="Displayed stack traces copied"
                                tooltip="Copy displayed stack traces"
                            />
                        )}
                    </Box>
                    {snapshotTime && (
                        <Typography variant="body2" color="text.secondary">
                            Snapshot at {snapshotTime.toLocaleTimeString()}
                        </Typography>
                    )}
                    {threadGroups && (
                        <Stack
                            direction={{ xs: 'column', md: 'row' }}
                            sx={{
                                alignItems: { xs: 'stretch', md: 'center' },
                                gap: { xs: 2, md: 1 },
                            }}
                        >
                            <Button variant="outlined" size="small" onClick={captureSnapshot} disabled={threadLoading}>
                                New Snapshot
                            </Button>
                            <FormControl size="small" sx={{ minWidth: 240 }}>
                                <InputLabel id="thread-group-label" sx={smallFormControlSx}>
                                    Group
                                </InputLabel>
                                <Select
                                    labelId="thread-group-label"
                                    label="Group"
                                    sx={smallFormControlSx}
                                    MenuProps={smallDropdownMenuPropsSx}
                                    value={selectedGroup}
                                    onChange={(event: SelectChangeEvent) => setSelectedGroup(event.target.value)}
                                >
                                    {Object.keys(threadGroups).map((group) => (
                                        <MenuItem key={group} value={group}>
                                            {group} ({filterThreads(threadGroups, group, selectedThreadState).length})
                                        </MenuItem>
                                    ))}
                                </Select>
                            </FormControl>
                            <FormControl size="small" sx={{ minWidth: 200 }}>
                                <InputLabel id="thread-state-label" sx={smallFormControlSx}>
                                    State
                                </InputLabel>
                                <Select
                                    labelId="thread-state-label"
                                    label="State"
                                    sx={smallFormControlSx}
                                    MenuProps={smallDropdownMenuPropsSx}
                                    value={selectedThreadState}
                                    onChange={(event: SelectChangeEvent) => setSelectedThreadState(event.target.value)}
                                >
                                    {THREAD_STATES.map((state) => (
                                        <MenuItem key={state} value={state}>
                                            {state} ({filterThreads(threadGroups, selectedGroup, state).length})
                                        </MenuItem>
                                    ))}
                                </Select>
                            </FormControl>
                        </Stack>
                    )}
                </Stack>
                <Divider />
            </Box>
            <Box sx={{ pt: 2 }}>{renderThreadSnapshotContent()}</Box>
        </>
    )
}
