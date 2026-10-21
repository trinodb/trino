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
import React, { useMemo, useState } from 'react'
import { Box, ButtonBase, IconButton, Stack, Tooltip, Typography } from '@mui/material'
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import { amber, blue, deepPurple, green, grey, lightBlue, orange, red } from '@mui/material/colors'
import { CodePreview } from './CodePreview.tsx'
import { QueryInfo, queryStatusApi } from '../api/webapp/api.ts'
import { Texts } from '../constant.ts'
import { useSnackbar } from './SnackbarContext.ts'
import { useConfigStore } from '../store'
import {
    formatDurationClock,
    getCompactStatusLabel,
    formatShortTime,
    getHumanReadableState,
    getQueryProgressPercentage,
    isKillable,
    parseAndFormatDataSize,
    parseDuration,
} from '../utils/utils.ts'
import { alpha } from '@mui/material/styles'
import { Link as RouterLink } from 'react-router-dom'
import { COMPACT_QUERY_GRID_SX, COMPACT_TABLE_PINNED_SX } from './compactQueryGrid.ts'

interface IQueryListItemProps {
    queryInfo: QueryInfo
    // Opens the shared kill confirmation (owned by QueryList)
    onKill: (queryInfo: QueryInfo) => void
}

// Query text in the list never takes more than this many rendered lines
const QUERY_PREVIEW_MAX_LINES = 6

// Server-side cap on queryTextPreview (TrimmedBasicQueryInfo.MAX_QUERY_PREVIEW_LENGTH); longer texts arrive cut
const QUERY_PREVIEW_SERVER_MAX_LENGTH = 300

// Removes the common indentation and blank lines. Length is capped visually by CodePreview's maxLines, which
// counts wrapped lines too, so a single very long line cannot take more space than six short ones.
const stripQueryTextWhitespace = (queryText: string) => {
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

    return lines
        .map((line) => line.substring(minLeadingWhitespace).replace(/\s+$/g, ''))
        .filter((line) => line.length > 0)
        .join('\n')
}

// Colour per state for the compact row's background tint
const getStatusColor = (queryInfo: QueryInfo): string => {
    switch (queryInfo.state) {
        case 'QUEUED':
        case 'WAITING_FOR_RESOURCES':
            return amber[700]
        case 'DISPATCHING':
        case 'PLANNING':
        case 'STARTING':
            return lightBlue[600]
        case 'RUNNING':
            return queryInfo.queryStats.fullyBlocked ? deepPurple[400] : blue[600]
        case 'FINISHING':
            return blue[600]
        case 'FINISHED':
            return green[600]
        case 'FAILED':
            return queryInfo.errorCode?.name === 'USER_CANCELED' ? orange[700] : red[600]
        default:
            return grey[500]
    }
}

// One cell of the compact row; the column header names it, the tooltip explains it
const CompactMetric = (props: { value: string; title: string }) => (
    <Box
        component="span"
        title={props.title}
        sx={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
    >
        {props.value}
    </Box>
)

// The list only carries a preview, cut by the server at MAX_QUERY_PREVIEW_LENGTH; fetch the full text only when
// the preview may be truncated
const fetchFullQueryText = async (queryInfo: QueryInfo): Promise<string> => {
    if (queryInfo.queryTextPreview.length < QUERY_PREVIEW_SERVER_MAX_LENGTH) {
        return queryInfo.queryTextPreview
    }
    const apiResponse = await queryStatusApi(queryInfo.queryId, true)
    if (apiResponse.status !== 200 || !apiResponse.data) {
        throw new Error(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
    }
    return apiResponse.data.query
}

// Query text capped at QUERY_PREVIEW_MAX_LINES. When text is hidden (by the line cap or the server's preview
// cut) a thin bar under it expands to the full query and collapses it again.
const QueryText = (props: { queryInfo: QueryInfo; formattedQueryText: string }) => {
    const { queryInfo, formattedQueryText } = props
    const { showSnackbar } = useSnackbar()
    const [expanded, setExpanded] = useState(false)
    const [clamped, setClamped] = useState(false)
    const [fullText, setFullText] = useState<string | null>(null)
    const previewCut = queryInfo.queryTextPreview.length >= QUERY_PREVIEW_SERVER_MAX_LENGTH

    const toggle = async () => {
        if (!expanded && fullText === null) {
            try {
                setFullText(stripQueryTextWhitespace(await fetchFullQueryText(queryInfo)))
            } catch (error) {
                showSnackbar((error as Error).message, 'error')
                return
            }
        }
        setExpanded(!expanded)
    }

    return (
        <Box sx={{ flexGrow: 1, minWidth: 0, display: 'flex', flexDirection: 'column' }}>
            <CodePreview
                code={expanded ? (fullText ?? formattedQueryText) : formattedQueryText}
                maxLines={expanded ? undefined : QUERY_PREVIEW_MAX_LINES}
                onClampedChange={setClamped}
            />
            {(expanded || clamped || previewCut) && (
                <ButtonBase
                    onClick={toggle}
                    disableRipple
                    aria-expanded={expanded}
                    aria-label={expanded ? Texts.QueryList.Actions.Collapse : Texts.QueryList.Actions.Expand}
                    title={expanded ? Texts.QueryList.Actions.Collapse : Texts.QueryList.Actions.Expand}
                    sx={(theme) => ({
                        height: 14,
                        bgcolor: alpha(theme.palette.text.primary, 0.06),
                        color: 'text.secondary',
                        '&:hover': { bgcolor: alpha(theme.palette.text.primary, 0.18) },
                    })}
                >
                    <ExpandMoreIcon sx={{ fontSize: 16, transform: expanded ? 'rotate(180deg)' : 'none' }} />
                </ButtonBase>
            )}
        </Box>
    )
}

// Narrow column of actions shown to the left of the query text; the kill confirmation lives in QueryList, shared by
// every row
const QueryActions = (props: { queryInfo: QueryInfo; onKill: (queryInfo: QueryInfo) => void }) => {
    const { queryInfo, onKill } = props
    const { showSnackbar } = useSnackbar()
    const canKill = isKillable(queryInfo)

    const copyQuery = async () => {
        let queryText: string
        try {
            queryText = await fetchFullQueryText(queryInfo)
        } catch (error) {
            showSnackbar((error as Error).message, 'error')
            return
        }
        try {
            await navigator.clipboard.writeText(queryText)
            showSnackbar(Texts.QueryList.Actions.Copied, 'success')
        } catch {
            showSnackbar(Texts.QueryList.Actions.CopyFailed, 'error')
        }
    }

    return (
        // Buttons side by side so a one-line query keeps its one-line height
        <Stack
            direction="row"
            sx={(theme) => ({
                flexShrink: 0,
                alignItems: 'flex-start',
                px: 0.25,
                py: 0.25,
                border: `1px solid ${theme.palette.mode === 'dark' ? '#3f3f3f' : '#ddd'}`,
                borderRight: 'none',
                borderBottom: 'none',
            })}
        >
            <Tooltip title={Texts.QueryList.Actions.Copy} placement="top">
                <IconButton size="small" onClick={copyQuery} aria-label={Texts.QueryList.Actions.Copy}>
                    <ContentCopyIcon sx={{ fontSize: 16 }} />
                </IconButton>
            </Tooltip>
            <Tooltip title={Texts.QueryList.Actions.Kill} placement="top">
                {/* span keeps the tooltip working while the button is disabled */}
                <span>
                    <IconButton
                        size="small"
                        color="error"
                        onClick={() => onKill(queryInfo)}
                        disabled={!canKill}
                        aria-label={Texts.QueryList.Actions.Kill}
                    >
                        <CancelOutlinedIcon sx={{ fontSize: 16 }} />
                    </IconButton>
                </span>
            </Tooltip>
        </Stack>
    )
}

// One clickable summary line on the shared table grid, then the query text
export const QueryListItem = React.memo(function QueryListItem(props: IQueryListItemProps) {
    const { queryInfo, onKill } = props
    const formattedQueryText = useMemo(
        () => stripQueryTextWhitespace(queryInfo.queryTextPreview),
        [queryInfo.queryTextPreview]
    )
    const stats = queryInfo.queryStats
    const statusColor = getStatusColor(queryInfo)
    // Running queries fill the row up to their progress; other states get a flat tint
    const progress = queryInfo.state === 'RUNNING' ? getQueryProgressPercentage(queryInfo) : 100
    const rowBackground = (filled: number, rest: number) =>
        `linear-gradient(to right, ${alpha(statusColor, filled)} ${progress}%, ${alpha(statusColor, rest)} ${progress}%)`
    const done = queryInfo.state === 'FINISHED' || queryInfo.state === 'FAILED'
    const hideQueryText = useConfigStore((state) => state.hideQueryText)

    return (
        <Stack spacing={0.5} sx={{ py: hideQueryText ? 0.25 : 0.5 }}>
            <Box
                component={RouterLink}
                to={`/queries/${queryInfo.queryId}`}
                title={`${getHumanReadableState(queryInfo)}${queryInfo.state === 'RUNNING' ? ` ${progress}%` : ''} · ${queryInfo.queryId}`}
                sx={{
                    ...COMPACT_QUERY_GRID_SX,
                    py: 0.5,
                    borderRadius: 1,
                    color: 'text.primary',
                    textDecoration: 'none',
                    // Tint keeps text readable in both themes; hover deepens it
                    background: rowBackground(0.32, 0.1),
                    '&:hover': { background: rowBackground(0.44, 0.18) },
                }}
            >
                <CompactMetric
                    value={formatShortTime(new Date(Date.parse(stats.createTime)))}
                    title={`Created ${new Date(Date.parse(stats.createTime)).toLocaleString()}`}
                />
                <CompactMetric
                    value={formatDurationClock(parseDuration(stats.elapsedTime))}
                    title="Total query wall time, including queued time"
                />
                <CompactMetric value={getCompactStatusLabel(queryInfo)} title={getHumanReadableState(queryInfo)} />
                <Box
                    component="span"
                    title={`User: ${queryInfo.sessionUser}`}
                    sx={{ fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                >
                    {queryInfo.sessionUser}
                </Box>
                <Box component="span" sx={{ color: 'text.disabled' }}>
                    |
                </Box>
                <CompactMetric
                    value={parseAndFormatDataSize(stats.totalMemoryReservation)}
                    title="Current total reserved memory"
                />
                <CompactMetric
                    value={parseAndFormatDataSize(stats.peakTotalMemoryReservation)}
                    title="Peak total memory"
                />
                <CompactMetric
                    value={formatDurationClock(parseDuration(stats.totalCpuTime))}
                    title="CPU time spent by this query"
                />
                {/* Empty cell under the header's "Splits" label */}
                <Box component="span" />
                {/* A finished or failed query reports no running or queued splits */}
                <CompactMetric value={String(stats.completedDrivers)} title="Finished splits" />
                <CompactMetric value={String(done ? 0 : stats.runningDrivers)} title="Running splits" />
                <CompactMetric value={String(done ? 0 : stats.queuedDrivers)} title="Queued splits" />
            </Box>
            {/* Pinned: stays in view while the rows scroll sideways. Hidden by the app bar's compact button, which
                leaves just the table (the error stays in the Status tooltip) */}
            {!hideQueryText && (
                <Box sx={COMPACT_TABLE_PINNED_SX}>
                    {queryInfo.state === 'FAILED' && (
                        <Typography variant="body2" sx={{ px: 1, pb: 0.5, color: 'error.main', fontWeight: 500 }}>
                            {getHumanReadableState(queryInfo)}
                        </Typography>
                    )}
                    <Box sx={{ display: 'flex', alignItems: 'stretch' }}>
                        <QueryActions queryInfo={queryInfo} onKill={onKill} />
                        <QueryText queryInfo={queryInfo} formattedQueryText={formattedQueryText} />
                    </Box>
                </Box>
            )}
        </Stack>
    )
})
