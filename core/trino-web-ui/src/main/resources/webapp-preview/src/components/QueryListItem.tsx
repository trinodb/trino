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
import React from 'react'
import { Box, Grid2 as Grid, Stack, Tooltip, Typography } from '@mui/material'
import AvTimerIcon from '@mui/icons-material/AvTimer'
import BadgeIcon from '@mui/icons-material/Badge'
import BrokenImageIcon from '@mui/icons-material/BrokenImage'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import DevicesIcon from '@mui/icons-material/Devices'
import DownloadingIcon from '@mui/icons-material/Downloading'
import FunctionsIcon from '@mui/icons-material/Functions'
import GroupsIcon from '@mui/icons-material/Groups'
import HighlightOff from '@mui/icons-material/HighlightOff'
import HistoryToggleOffIcon from '@mui/icons-material/HistoryToggleOff'
import Memory from '@mui/icons-material/Memory'
import NotStartedIcon from '@mui/icons-material/NotStarted'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import QueryBuilderIcon from '@mui/icons-material/QueryBuilder'
import { CodeBlock } from './CodeBlock.tsx'
import { LinearProgressWithLabel, LinearProgressWithLabelProps } from './LinearProgressWithLabel.tsx'
import { QueryInfo } from '../api/webapp/api.ts'
import {
    formatDataSizeBytes,
    formatShortTime,
    getHumanReadableState,
    parseAndFormatDataSize,
    truncateString,
} from '../utils/utils.ts'

interface IQueryListItemProps {
    queryInfo: QueryInfo
}

export const QueryListItem = (props: IQueryListItemProps) => {
    const { queryInfo } = props

    const STATE_COLOR_MAP: Record<string, LinearProgressWithLabelProps['color']> = {
        QUEUED: 'default',
        RUNNING: 'info',
        PLANNING: 'info',
        FINISHED: 'success',
        BLOCKED: 'secondary',
        USER_ERROR: 'error',
        CANCELED: 'warning',
        INSUFFICIENT_RESOURCES: 'error',
        EXTERNAL_ERROR: 'error',
        UNKNOWN_ERROR: 'error',
    }

    const getQueryStateColor = (query: QueryInfo): LinearProgressWithLabelProps['color'] => {
        switch (query.state) {
            case 'QUEUED':
                return STATE_COLOR_MAP.QUEUED
            case 'PLANNING':
                return STATE_COLOR_MAP.PLANNING
            case 'STARTING':
            case 'FINISHING':
            case 'RUNNING':
                if (query.queryStats && query.queryStats.fullyBlocked) {
                    return STATE_COLOR_MAP.BLOCKED
                }
                return STATE_COLOR_MAP.RUNNING
            case 'FAILED':
                switch (query.errorType) {
                    case 'USER_ERROR':
                        if (query.errorCode.name === 'USER_CANCELED') {
                            return STATE_COLOR_MAP.CANCELED
                        }
                        return STATE_COLOR_MAP.USER_ERROR
                    case 'EXTERNAL':
                        return STATE_COLOR_MAP.EXTERNAL_ERROR
                    case 'INSUFFICIENT_RESOURCES':
                        return STATE_COLOR_MAP.INSUFFICIENT_RESOURCES
                    default:
                        return STATE_COLOR_MAP.UNKNOWN_ERROR
                }
            case 'FINISHED':
                return STATE_COLOR_MAP.FINISHED
            default:
                return STATE_COLOR_MAP.QUEUED
        }
    }

    const getProgressBarPercentage = (queryInfo: QueryInfo) => {
        if (queryInfo.state !== 'RUNNING') {
            return 100
        }

        const progress = queryInfo.queryStats.progressPercentage || 0
        return Math.round(progress)
    }

    const getProgressBarTitle = (queryInfo: QueryInfo) => {
        return getHumanReadableState(queryInfo)
    }

    const stripQueryTextWhitespace = (queryText: string) => {
        const maxLines = 6
        const lines = queryText.split('\n')
        let minLeadingWhitespace = -1
        for (let i = 0; i < lines.length; i++) {
            if (minLeadingWhitespace === 0) {
                break
            }

            if (lines[i].trim().length === 0) {
                continue
            }

            const leadingWhitespace = lines[i].search(/\S/)

            if (leadingWhitespace > -1 && (leadingWhitespace < minLeadingWhitespace || minLeadingWhitespace === -1)) {
                minLeadingWhitespace = leadingWhitespace
            }
        }

        let formattedQueryText = ''

        for (let i = 0; i < lines.length; i++) {
            const trimmedLine = lines[i].substring(minLeadingWhitespace).replace(/\s+$/g, '')

            if (trimmedLine.length > 0) {
                formattedQueryText += trimmedLine
                if (i < maxLines - 1) {
                    formattedQueryText += '\n'
                } else {
                    formattedQueryText += '\n...'
                    break
                }
            }
        }

        return formattedQueryText
    }

    const renderTextWithIcon = (
        icon: React.ReactElement,
        title: string,
        tooltip: string,
        spacing: number = 0,
        color: string = 'inherit'
    ) => {
        const smallIcon = React.cloneElement(icon, { fontSize: 'small', color: color })

        return (
            <Tooltip placement="top-start" title={tooltip}>
                <Box display="flex" alignItems="center">
                    {smallIcon}
                    <Typography variant="body2" ml={spacing}>
                        {title}
                    </Typography>
                </Box>
            </Tooltip>
        )
    }

    return (
        <Grid container spacing={1}>
            <Grid size={{ xs: 12, lg: 4 }}>
                <Box
                    sx={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        pb: 1,
                        mb: 1,
                        borderBottom: '1px solid #ccc',
                    }}
                >
                    <Box>
                        <Tooltip placement="top-start" title="Query ID">
                            <Typography variant="subtitle2">{queryInfo.queryId}</Typography>
                        </Tooltip>
                    </Box>
                    <Box>
                        <Tooltip placement="top-start" title="Submit time">
                            <Typography variant="body2">
                                {formatShortTime(new Date(Date.parse(queryInfo.queryStats.createTime)))}
                            </Typography>
                        </Tooltip>
                    </Box>
                </Box>
                <Box>
                    <Box>
                        {renderTextWithIcon(
                            <BadgeIcon />,
                            truncateString(queryInfo.sessionUser, 35),
                            'User',
                            1,
                            'info'
                        )}
                    </Box>
                    <Box>
                        {renderTextWithIcon(
                            <DevicesIcon />,
                            truncateString(queryInfo.sessionSource, 35),
                            'Source',
                            1,
                            'info'
                        )}
                    </Box>
                    <Box>
                        {renderTextWithIcon(
                            <DownloadingIcon />,
                            queryInfo.queryDataEncoding ? 'spooled ' + queryInfo.queryDataEncoding : 'non-spooled',
                            'Protocol encoding',
                            1,
                            'info'
                        )}
                    </Box>
                    <Box>
                        {renderTextWithIcon(
                            <GroupsIcon />,
                            truncateString(queryInfo.resourceGroupId ? queryInfo.resourceGroupId.join('.') : 'n/a', 35),
                            'Resource group',
                            1,
                            'info'
                        )}
                    </Box>
                </Box>
                <Grid container spacing={3}>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <CheckCircleIcon />,
                            queryInfo.queryStats.completedDrivers.toString(),
                            'Complete splits',
                            1
                        )}
                    </Grid>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <PlayCircleIcon />,
                            (queryInfo.state === 'FINISHED' || queryInfo.state === 'FAILED'
                                ? 0
                                : queryInfo.queryStats.runningDrivers
                            ).toString(),
                            'Running splits',
                            1
                        )}
                    </Grid>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <NotStartedIcon />,
                            (queryInfo.state === 'FINISHED' || queryInfo.state === 'FAILED'
                                ? 0
                                : queryInfo.queryStats.queuedDrivers
                            ).toString(),
                            'Queued splits',
                            1
                        )}
                    </Grid>
                    {queryInfo.retryPolicy === 'TASK' && (
                        <Grid size={{ xs: 3 }}>
                            {renderTextWithIcon(
                                <HighlightOff />,
                                queryInfo.queryStats.failedTasks.toString(),
                                'Failed tasks',
                                1
                            )}
                        </Grid>
                    )}
                </Grid>
                <Grid container spacing={3}>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <HistoryToggleOffIcon />,
                            queryInfo.queryStats.executionTime.toString(),
                            'Wall time spent executing the query (not including queued time)',
                            1
                        )}
                    </Grid>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <QueryBuilderIcon />,
                            queryInfo.queryStats.elapsedTime.toString(),
                            'Total query wall time',
                            1
                        )}
                    </Grid>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <AvTimerIcon />,
                            queryInfo.queryStats.totalCpuTime.toString(),
                            'CPU time spent by this query',
                            1
                        )}
                    </Grid>
                </Grid>
                <Grid container spacing={3}>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <Memory />,
                            parseAndFormatDataSize(queryInfo.queryStats.totalMemoryReservation),
                            'Current total reserved memory',
                            1
                        )}
                    </Grid>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <BrokenImageIcon />,
                            parseAndFormatDataSize(queryInfo.queryStats.peakTotalMemoryReservation),
                            'Peak total memory',
                            1
                        )}
                    </Grid>
                    <Grid size={{ xs: 3 }}>
                        {renderTextWithIcon(
                            <FunctionsIcon />,
                            formatDataSizeBytes(queryInfo.queryStats.cumulativeUserMemory / 1000.0),
                            'Cumulative user memory',
                            1
                        )}
                    </Grid>
                </Grid>
            </Grid>
            <Grid size={{ xs: 12, lg: 8 }}>
                <Stack flex={1} spacing={1}>
                    <LinearProgressWithLabel
                        value={getProgressBarPercentage(queryInfo)}
                        title={getProgressBarTitle(queryInfo)}
                        color={getQueryStateColor(queryInfo)}
                    />
                    <Grid container>
                        <Grid
                            key={queryInfo.queryId}
                            display="flex"
                            flexGrow={1}
                            alignItems="flex-start"
                            sx={{
                                width: '100%',
                            }}
                        >
                            <CodeBlock
                                language="sql"
                                code={stripQueryTextWhitespace(queryInfo.queryTextPreview)}
                                height="158px"
                            />
                        </Grid>
                    </Grid>
                </Stack>
            </Grid>
        </Grid>
    )
}
