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
import { useEffect, useRef, useState, type ComponentProps, type HTMLAttributes, type Ref } from 'react'
import { Alert, Box, CircularProgress, Divider, Grid, Tooltip, Typography } from '@mui/material'
import { blue, green, purple, teal } from '@mui/material/colors'
import { darken, useTheme } from '@mui/material/styles'
import Timeline, { type TimelineGroupBase, type TimelineItemBase } from 'react-calendar-timeline'
import { queryStatusApi, QueryStage, QueryStatusInfo, QueryTask } from '../api/webapp/api.ts'
import { Texts } from '../constant.ts'
import { ApiResponse } from '../api/base.ts'
import { getTaskIdSuffix } from '../utils/utils'
import { QueryProgressBar } from './QueryProgressBar'
import 'react-calendar-timeline/dist/style.css'

interface IQueryStatus {
    info: QueryStatusInfo | null
}

type SplitTimelineItem = TimelineItemBase<number> & {
    color: string
    bgColor: string
    borderColor: string
}

type TimelineItemRenderer = NonNullable<ComponentProps<typeof Timeline>['itemRenderer']>
type TimelineItemRendererProps = Parameters<TimelineItemRenderer>[0]

export const QuerySplitsTimeline = () => {
    const { queryId } = useParams()
    const theme = useTheme()
    const initialQueryStatus: IQueryStatus = {
        info: null,
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
            queryStatusApi(queryId).then((apiResponse: ApiResponse<QueryStatusInfo>) => {
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    setQueryStatus({
                        info: apiResponse.data,
                    })
                    setError(null)
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
        }
    }

    const renderSplitsTimeline = (stages: QueryStage[]) => {
        const tasks = stages
            .flatMap((stage) => stage.tasks)
            .map((task: QueryTask) => ({
                taskId: getTaskIdSuffix(task.taskStatus.taskId),
                time: {
                    create: task.stats.createTime,
                    firstStart: task.stats.firstStartTime,
                    lastStart: task.stats.lastStartTime,
                    lastEnd: task.stats.lastEndTime,
                    end: task.stats.endTime,
                },
            }))

        const groups: TimelineGroupBase[] = []
        const items: SplitTimelineItem[] = []
        const timelineColors =
            theme.palette.mode === 'light'
                ? {
                      created: teal[300],
                      firstSplitStarted: purple[300],
                      lastSplitStarted: blue[300],
                      lastSplitEnded: green[300],
                  }
                : {
                      created: teal[700],
                      firstSplitStarted: purple[600],
                      lastSplitStarted: blue[600],
                      lastSplitEnded: green[600],
                  }
        const legendEntries = [
            { key: 'created', label: 'Task created', color: timelineColors.created },
            { key: 'firstSplitStarted', label: 'First split started', color: timelineColors.firstSplitStarted },
            { key: 'lastSplitStarted', label: 'Last split started', color: timelineColors.lastSplitStarted },
            { key: 'lastSplitEnded', label: 'Last split ended', color: timelineColors.lastSplitEnded },
        ]
        const applyColorProps = (background: string) => ({
            bgColor: background,
            color: theme.palette.getContrastText(background),
            borderColor: darken(background, 0.5),
        })
        for (let i = 0; i < tasks.length; i++) {
            const task = tasks[i]
            const stageId = task.taskId.substring(0, task.taskId.indexOf('.'))
            const taskNumber = getTaskIdSuffix(task.taskId)

            if (taskNumber === '0.0') {
                groups.push({
                    id: stageId,
                    title: `Stage ${stageId}`,
                })
            }
            items.push({
                id: `${task.taskId} - created`,
                group: stageId,
                title: `${task.taskId} - created`,
                start_time: new Date(task.time.create).getTime(),
                end_time: new Date(task.time.firstStart).getTime(),
                ...applyColorProps(timelineColors.created),
            })
            items.push({
                id: `${task.taskId} - first split started`,
                group: stageId,
                title: `${task.taskId} - first split started`,
                start_time: new Date(task.time.firstStart).getTime(),
                end_time: new Date(task.time.lastStart).getTime(),
                ...applyColorProps(timelineColors.firstSplitStarted),
            })
            items.push({
                id: `${task.taskId} - last split started`,
                group: stageId,
                title: `${task.taskId} - last split started`,
                start_time: new Date(task.time.lastStart).getTime(),
                end_time: new Date(task.time.lastEnd).getTime(),
                ...applyColorProps(timelineColors.lastSplitStarted),
            })
            items.push({
                id: `${task.taskId} - last split ended`,
                group: stageId,
                title: `${task.taskId} - last split ended`,
                start_time: new Date(task.time.lastEnd).getTime(),
                end_time: new Date(task.time.end).getTime(),
                ...applyColorProps(timelineColors.lastSplitEnded),
            })
        }

        if (items.length === 0) {
            return <Alert severity="info">No split timeline data available.</Alert>
        }

        const startTimes = items.map((i) => i.start_time).filter(Number.isFinite)
        const endTimes = items.map((i) => i.end_time).filter(Number.isFinite)

        const timelineStartTime = startTimes.length ? Math.min(...startTimes) : null
        const timelineEndTime = endTimes.length ? Math.max(...endTimes) : null

        const itemRenderer: TimelineItemRenderer = ({ item, itemContext, getItemProps }: TimelineItemRendererProps) => {
            const splitItem = item as SplitTimelineItem
            const itemProps = getItemProps({
                style: {
                    color: splitItem.color,
                    backgroundColor: splitItem.bgColor,
                    borderColor: splitItem.borderColor,
                    borderStyle: 'solid',
                    borderWidth: 1,
                    borderRadius: 4,
                    borderLeftWidth: 1,
                    borderRightWidth: 1,
                },
            })
            const { key: itemKey, title, ref: itemRef, ...restItemProps } = itemProps
            // Drop the react-calendar-timeline provided title to avoid default tooltips; MUI tooltip will handle hover info.
            delete (restItemProps as { title?: string }).title
            const timeFormatter = new Intl.DateTimeFormat(undefined, {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            })
            const formattedStart = timeFormatter.format(new Date(splitItem.start_time))
            const formattedEnd = timeFormatter.format(new Date(splitItem.end_time))
            return (
                <Tooltip
                    placement="top"
                    title={
                        <Grid container spacing={0.5} textAlign="center" justifyContent="center">
                            <Grid size={{ xs: 12 }}>
                                <Typography variant="subtitle2">{title}</Typography>
                            </Grid>
                            <Grid size={{ xs: 12 }}>
                                <Typography variant="caption">{`${formattedStart} – ${formattedEnd}`}</Typography>
                            </Grid>
                        </Grid>
                    }
                >
                    <Box
                        key={itemKey ?? splitItem.id}
                        ref={itemRef as Ref<HTMLDivElement>}
                        {...(restItemProps as HTMLAttributes<HTMLDivElement>)}
                    >
                        <Box
                            style={{
                                height: itemContext.dimensions.height,
                                overflow: 'hidden',
                                paddingLeft: 3,
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                            }}
                        >
                            {itemContext.title}
                        </Box>
                    </Box>
                </Tooltip>
            )
        }

        return (
            <Box
                sx={{
                    mt: 1,
                    '& .react-calendar-timeline .rct-header-root': {
                        backgroundColor:
                            theme.palette.mode === 'light' ? theme.palette.grey[100] : theme.palette.background.paper,
                        color: theme.palette.text.primary,
                        borderBottomColor: theme.palette.divider,
                    },
                    '& .react-calendar-timeline .rct-dateHeader': {
                        backgroundColor: theme.palette.background.default,
                        color: theme.palette.text.secondary,
                        borderLeftColor: theme.palette.divider,
                        borderBottomColor: theme.palette.divider,
                    },
                    '& .react-calendar-timeline .rct-dateHeader-primary': {
                        backgroundColor: theme.palette.background.paper,
                        color: theme.palette.text.primary,
                        borderColor: theme.palette.divider,
                    },
                    '& .react-calendar-timeline .rct-vertical-lines .rct-vl': {
                        borderLeftColor: theme.palette.divider, // darker/custom color
                        // borderLeft: 'none', // or comment this out and use display:'none' to remove
                    },
                }}
            >
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, alignItems: 'center', mb: 1 }}>
                    {legendEntries.map((entry) => (
                        <Box key={entry.key} sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Box
                                sx={{
                                    width: 14,
                                    height: 14,
                                    borderRadius: 0.5,
                                    bgcolor: entry.color,
                                    border: '1px solid',
                                    borderColor: theme.palette.divider,
                                }}
                            />
                            <Typography variant="body2" color="text.secondary">
                                {entry.label}
                            </Typography>
                        </Box>
                    ))}
                </Box>

                {timelineStartTime && timelineEndTime ? (
                    <Timeline
                        groups={groups}
                        items={items}
                        defaultTimeStart={timelineStartTime}
                        defaultTimeEnd={timelineEndTime}
                        itemRenderer={itemRenderer}
                        minZoom={60 * 1000}
                        itemHeightRatio={0.65}
                        itemTouchSendsClick={false}
                        canMove={false}
                        canResize={false}
                        stackItems
                        // traditionalZoom
                    />
                ) : (
                    <Box sx={{ width: '100%', mt: 1 }}>
                        <Alert severity="info">
                            Splits timeline will appear automatically when at least one query task starts running
                        </Alert>
                    </Box>
                )}
            </Box>
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

                            {queryStatus.info?.stages.stages ? (
                                <Grid container spacing={3}>
                                    <Grid size={{ xs: 12, md: 12 }}>
                                        <Box sx={{ pt: 2 }}>
                                            <Typography variant="h6">Splits timeline</Typography>
                                            <Divider />
                                        </Box>
                                        {renderSplitsTimeline(queryStatus.info.stages.stages)}
                                    </Grid>
                                </Grid>
                            ) : (
                                <>
                                    <Box sx={{ width: '100%', mt: 1 }}>
                                        <Alert severity="info">
                                            Splits timeline will appear automatically when query starts running.
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
