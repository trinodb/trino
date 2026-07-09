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
import { Box, Grid, Stack, Tooltip, Typography } from '@mui/material'
import type { SvgIconComponent } from '@mui/icons-material'
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
import { CodePreview } from './CodePreview.tsx'
import { QueryProgressBar } from './QueryProgressBar'
import { QueryInfo } from '../api/webapp/api.ts'
import { formatDataSizeBytes, formatShortTime, parseAndFormatDataSize, truncateString } from '../utils/utils.ts'
import { styled } from '@mui/material/styles'
import { Link as RouterLink } from 'react-router-dom'
import { SvgIconProps } from '@mui/material/SvgIcon'

interface IQueryListItemProps {
    queryInfo: QueryInfo
}

const StyledLink = styled(RouterLink)(({ theme }) => ({
    textDecoration: 'none',
    color: theme.palette.info.main,
    '&:hover': {
        textDecoration: 'underline',
    },
    fontSize: 14,
    fontWeight: 500,
}))

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

// Lightweight memoized replacement for the previous renderTextWithIcon function.
// Uses the icon component type directly (no React.cloneElement) and native title
// attribute instead of MUI Tooltip to minimise DOM weight per query row.
const TextWithIcon = React.memo(function TextWithIcon(props: {
    Icon: SvgIconComponent
    title: string
    tooltip: string
    spacing?: number
    color?: SvgIconProps['color']
}) {
    const { Icon, title, tooltip, spacing = 0, color = 'inherit' } = props
    return (
        <Box sx={{ display: 'flex', alignItems: 'center' }} title={tooltip} aria-label={`${tooltip}: ${title}`}>
            <Icon fontSize="small" color={color} />
            <Typography variant="body2" sx={{ ml: spacing }}>
                {title}
            </Typography>
        </Box>
    )
})

// --- Memoized sub-sections of the left pane ---

const QueryHeader = React.memo(function QueryHeader(props: { queryId: string; createTime: string }) {
    const { queryId, createTime } = props
    return (
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
                    <StyledLink to={`/queries/${queryId}`}>{queryId}</StyledLink>
                </Tooltip>
            </Box>
            <Box>
                <Tooltip placement="top-start" title="Submit time">
                    <Typography variant="body2">{formatShortTime(new Date(Date.parse(createTime)))}</Typography>
                </Tooltip>
            </Box>
        </Box>
    )
})

const QueryIdentity = React.memo(
    function QueryIdentity(props: {
        sessionUser: string
        sessionSource: string
        queryDataEncoding: string
        resourceGroupId: string[]
    }) {
        const { sessionUser, sessionSource, queryDataEncoding, resourceGroupId } = props
        return (
            <Box>
                <Box>
                    <TextWithIcon
                        Icon={BadgeIcon}
                        title={truncateString(sessionUser, 35)}
                        tooltip="User"
                        spacing={1}
                        color="info"
                    />
                </Box>
                <Box>
                    <TextWithIcon
                        Icon={DevicesIcon}
                        title={truncateString(sessionSource, 35)}
                        tooltip="Source"
                        spacing={1}
                        color="info"
                    />
                </Box>
                <Box>
                    <TextWithIcon
                        Icon={DownloadingIcon}
                        title={queryDataEncoding ? 'spooled ' + queryDataEncoding : 'non-spooled'}
                        tooltip="Protocol encoding"
                        spacing={1}
                        color="info"
                    />
                </Box>
                <Box>
                    <TextWithIcon
                        Icon={GroupsIcon}
                        title={truncateString(resourceGroupId ? resourceGroupId.join('.') : 'n/a', 35)}
                        tooltip="Resource group"
                        spacing={1}
                        color="info"
                    />
                </Box>
            </Box>
        )
    },
    (prev, next) =>
        prev.sessionUser === next.sessionUser &&
        prev.sessionSource === next.sessionSource &&
        prev.queryDataEncoding === next.queryDataEncoding &&
        prev.resourceGroupId?.join('.') === next.resourceGroupId?.join('.')
)

const QuerySplits = React.memo(function QuerySplits(props: {
    completedDrivers: number
    runningDrivers: number
    queuedDrivers: number
    failedTasks: number
    state: string
    retryPolicy: string
}) {
    const { completedDrivers, runningDrivers, queuedDrivers, failedTasks, state, retryPolicy } = props
    const isDone = state === 'FINISHED' || state === 'FAILED'
    return (
        <Grid container spacing={3}>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={CheckCircleIcon}
                    title={completedDrivers.toString()}
                    tooltip="Complete splits"
                    spacing={1}
                />
            </Grid>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={PlayCircleIcon}
                    title={(isDone ? 0 : runningDrivers).toString()}
                    tooltip="Running splits"
                    spacing={1}
                />
            </Grid>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={NotStartedIcon}
                    title={(isDone ? 0 : queuedDrivers).toString()}
                    tooltip="Queued splits"
                    spacing={1}
                />
            </Grid>
            {retryPolicy === 'TASK' && (
                <Grid size={{ xs: 3 }}>
                    <TextWithIcon
                        Icon={HighlightOff}
                        title={failedTasks.toString()}
                        tooltip="Failed tasks"
                        spacing={1}
                    />
                </Grid>
            )}
        </Grid>
    )
})

const QueryTiming = React.memo(function QueryTiming(props: {
    executionTime: string
    elapsedTime: string
    totalCpuTime: string
}) {
    const { executionTime, elapsedTime, totalCpuTime } = props
    return (
        <Grid container spacing={3}>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={HistoryToggleOffIcon}
                    title={executionTime.toString()}
                    tooltip="Wall time spent executing the query (not including queued time)"
                    spacing={1}
                />
            </Grid>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={QueryBuilderIcon}
                    title={elapsedTime.toString()}
                    tooltip="Total query wall time"
                    spacing={1}
                />
            </Grid>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={AvTimerIcon}
                    title={totalCpuTime.toString()}
                    tooltip="CPU time spent by this query"
                    spacing={1}
                />
            </Grid>
        </Grid>
    )
})

const QueryMemory = React.memo(function QueryMemory(props: {
    totalMemoryReservation: string
    peakTotalMemoryReservation: string
    cumulativeUserMemory: number
}) {
    const { totalMemoryReservation, peakTotalMemoryReservation, cumulativeUserMemory } = props
    return (
        <Grid container spacing={3}>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={Memory}
                    title={parseAndFormatDataSize(totalMemoryReservation)}
                    tooltip="Current total reserved memory"
                    spacing={1}
                />
            </Grid>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={BrokenImageIcon}
                    title={parseAndFormatDataSize(peakTotalMemoryReservation)}
                    tooltip="Peak total memory"
                    spacing={1}
                />
            </Grid>
            <Grid size={{ xs: 3 }}>
                <TextWithIcon
                    Icon={FunctionsIcon}
                    title={formatDataSizeBytes(cumulativeUserMemory / 1000.0)}
                    tooltip="Cumulative user memory"
                    spacing={1}
                />
            </Grid>
        </Grid>
    )
})

export const QueryListItem = React.memo(function QueryListItem(props: IQueryListItemProps) {
    const { queryInfo } = props

    const formattedQueryText = useMemo(
        () => stripQueryTextWhitespace(queryInfo.queryTextPreview),
        [queryInfo.queryTextPreview]
    )

    return (
        <Grid container spacing={1}>
            <Grid size={{ xs: 12, lg: 4 }}>
                <QueryHeader queryId={queryInfo.queryId} createTime={queryInfo.queryStats.createTime} />
                <QueryIdentity
                    sessionUser={queryInfo.sessionUser}
                    sessionSource={queryInfo.sessionSource}
                    queryDataEncoding={queryInfo.queryDataEncoding}
                    resourceGroupId={queryInfo.resourceGroupId}
                />
                <QuerySplits
                    completedDrivers={queryInfo.queryStats.completedDrivers}
                    runningDrivers={queryInfo.queryStats.runningDrivers}
                    queuedDrivers={queryInfo.queryStats.queuedDrivers}
                    failedTasks={queryInfo.queryStats.failedTasks}
                    state={queryInfo.state}
                    retryPolicy={queryInfo.retryPolicy}
                />
                <QueryTiming
                    executionTime={queryInfo.queryStats.executionTime}
                    elapsedTime={queryInfo.queryStats.elapsedTime}
                    totalCpuTime={queryInfo.queryStats.totalCpuTime}
                />
                <QueryMemory
                    totalMemoryReservation={queryInfo.queryStats.totalMemoryReservation}
                    peakTotalMemoryReservation={queryInfo.queryStats.peakTotalMemoryReservation}
                    cumulativeUserMemory={queryInfo.queryStats.cumulativeUserMemory}
                />
            </Grid>
            <Grid size={{ xs: 12, lg: 8 }}>
                <Stack spacing={1} sx={{ flex: 1 }}>
                    <QueryProgressBar queryInfoBase={queryInfo} />
                    <Grid container>
                        <Grid
                            key={queryInfo.queryId}
                            sx={{
                                display: 'flex',
                                flexGrow: 1,
                                alignItems: 'flex-start',
                                width: '100%',
                            }}
                        >
                            <CodePreview code={formattedQueryText} height="158px" />
                        </Grid>
                    </Grid>
                </Stack>
            </Grid>
        </Grid>
    )
})
