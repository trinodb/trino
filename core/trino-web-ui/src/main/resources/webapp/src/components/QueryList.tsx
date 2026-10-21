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
import { useEffect, useRef, useState } from 'react'
import {
    Alert,
    Box,
    Button,
    ButtonBase,
    Checkbox,
    CircularProgress,
    Divider,
    FormControl,
    Grid,
    InputLabel,
    ListItemText,
    MenuItem,
    Select,
    Stack,
    Switch,
    Typography,
    useMediaQuery,
} from '@mui/material'
import { useTheme } from '@mui/material/styles'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import { DebouncedTextField } from './DebouncedTextField.tsx'
import { QueryListItem } from './QueryListItem.tsx'
import { useAuth } from './AuthContext'
import { KillQueryDialog } from './KillQueryDialog.tsx'
import { ApiResponse } from '../api/base.ts'
import { APP_BAR_HEIGHT, Texts } from '../constant.ts'
import { QueryInfo, queryApi } from '../api/webapp/api.ts'
import { useClusterStatsStore } from '../store'
import { updateQueryCpuShares } from '../utils/queryCpuShare.ts'
import { pollCoordinator } from '../utils/poll.ts'
import { getCompactStatusRank, getHumanReadableState, parseDataSize, parseDuration } from '../utils/utils.ts'
import { COMPACT_QUERY_GRID_SX, COMPACT_TABLE_CONTENT_SX, COMPACT_TABLE_SCROLLER_SX } from './compactQueryGrid.ts'

// Query list: one request at a time, at most this often (the response lists every query the coordinator remembers)
const QUERY_LIST_INTERVAL_MS = 1_500

const STATE_TYPE = {
    RUNNING: (queryInfo: QueryInfo) =>
        !(queryInfo.state === 'QUEUED' || queryInfo.state === 'FINISHED' || queryInfo.state === 'FAILED'),
    QUEUED: (queryInfo: QueryInfo) => queryInfo.state === 'QUEUED',
    FINISHED: (queryInfo: QueryInfo) => queryInfo.state === 'FINISHED',
} as const

const ERROR_TYPE = {
    USER_ERROR: (queryInfo: QueryInfo) => queryInfo.state === 'FAILED' && queryInfo.errorType === 'USER_ERROR',
    INTERNAL_ERROR: (queryInfo: QueryInfo) => queryInfo.state === 'FAILED' && queryInfo.errorType === 'INTERNAL_ERROR',
    INSUFFICIENT_RESOURCES: (queryInfo: QueryInfo) =>
        queryInfo.state === 'FAILED' && queryInfo.errorType === 'INSUFFICIENT_RESOURCES',
    EXTERNAL: (queryInfo: QueryInfo) => queryInfo.state === 'FAILED' && queryInfo.errorType === 'EXTERNAL',
} as const

const ACTIVE_QUERY_STATES = [
    'QUEUED',
    'WAITING_FOR_RESOURCES',
    'DISPATCHING',
    'PLANNING',
    'STARTING',
    'RUNNING',
    'FINISHING',
] as const

const SORT_TYPE = {
    PROGRESS: (queryInfo: QueryInfo) => {
        const index = ACTIVE_QUERY_STATES.findIndex((state) => state === queryInfo.state)
        if (index !== -1) {
            return (
                (ACTIVE_QUERY_STATES.length - index) * 1e15 +
                (100 - (queryInfo.queryStats.progressPercentage ?? 100)) * 1e12 +
                Date.parse(queryInfo.queryStats.createTime)
            )
        }
        return Date.parse(queryInfo.queryStats.endTime)
    },
    CREATED: (queryInfo: QueryInfo) => Date.parse(queryInfo.queryStats.createTime),
    ELAPSED: (queryInfo: QueryInfo) => parseDuration(queryInfo.queryStats.elapsedTime),
    EXECUTION: (queryInfo: QueryInfo) => parseDuration(queryInfo.queryStats.executionTime),
    CPU: (queryInfo: QueryInfo) => parseDuration(queryInfo.queryStats.totalCpuTime),
    // Queries without a current rate (done, or just appeared) sort as 0
    CPU_CURRENT: (queryInfo: QueryInfo) => queryInfo.cpuShare ?? 0,
    CUMULATIVE_MEMORY: (queryInfo: QueryInfo) => queryInfo.queryStats.cumulativeUserMemory,
    CURRENT_MEMORY: (queryInfo: QueryInfo) => parseDataSize(queryInfo.queryStats.userMemoryReservation),
    TOTAL_MEMORY: (queryInfo: QueryInfo) => parseDataSize(queryInfo.queryStats.totalMemoryReservation),
    PEAK_MEMORY: (queryInfo: QueryInfo) => parseDataSize(queryInfo.queryStats.peakTotalMemoryReservation),
    USER: (queryInfo: QueryInfo) => queryInfo.sessionUser,
    STATUS: (queryInfo: QueryInfo) => getCompactStatusRank(queryInfo),
    FINISHED_SPLITS: (queryInfo: QueryInfo) => queryInfo.queryStats.completedDrivers,
    RUNNING_SPLITS: (queryInfo: QueryInfo) => queryInfo.queryStats.runningDrivers,
    QUEUED_SPLITS: (queryInfo: QueryInfo) => queryInfo.queryStats.queuedDrivers,
} as const

// Header cells of the compact list, in column order; null is a separator column, a string is a group label whose
// column stays empty in the rows
const COMPACT_HEADER_COLUMNS: ({ sortType: keyof typeof SORT_TYPE; label: string; title?: string } | string | null)[] =
    [
        { sortType: 'CREATED', label: 'Created' },
        { sortType: 'ELAPSED', label: 'Wall', title: 'Total wall time, including queued time' },
        { sortType: 'STATUS', label: 'Status' },
        { sortType: 'USER', label: 'User' },
        null,
        { sortType: 'TOTAL_MEMORY', label: 'Mem', title: 'Current total reserved memory' },
        { sortType: 'PEAK_MEMORY', label: 'Peak Mem', title: 'Peak total memory' },
        { sortType: 'CPU', label: 'CPU', title: 'CPU time' },
        {
            sortType: 'CPU_CURRENT',
            label: 'CPU Current',
            title: 'Share of all cluster cores this query is using now (last 10 s)',
        },
        'Splits',
        { sortType: 'FINISHED_SPLITS', label: 'Finished', title: 'Finished splits' },
        { sortType: 'RUNNING_SPLITS', label: 'Running', title: 'Running splits' },
        { sortType: 'QUEUED_SPLITS', label: 'Queued', title: 'Queued splits' },
    ]

// Column headers of the compact rows: same grid, click a column to sort by it
const CompactQueryHeader = (props: {
    sortType: keyof typeof SORT_TYPE
    sortOrder: keyof typeof SORT_ORDER
    onSort: (sortType: keyof typeof SORT_TYPE) => void
}) => (
    <Box sx={{ ...COMPACT_QUERY_GRID_SX, py: 0.5, color: 'text.secondary', fontWeight: 500 }}>
        {COMPACT_HEADER_COLUMNS.map((column, index) =>
            column && typeof column === 'object' ? (
                <ButtonBase
                    key={column.sortType}
                    disableRipple
                    title={column.title}
                    onClick={() => props.onSort(column.sortType)}
                    aria-sort={
                        props.sortType === column.sortType
                            ? props.sortOrder === 'ASCENDING'
                                ? 'ascending'
                                : 'descending'
                            : undefined
                    }
                    sx={{
                        justifyContent: 'flex-start',
                        font: 'inherit',
                        color: props.sortType === column.sortType ? 'text.primary' : 'inherit',
                        borderRadius: 0.5,
                        '&:hover': { color: 'text.primary' },
                    }}
                >
                    {column.label}
                    {/* ↕ marks every sortable column; the active one shows its direction */}
                    <Box component="span" sx={{ ml: 0.5, opacity: props.sortType === column.sortType ? 1 : 0.5 }}>
                        {props.sortType === column.sortType ? (props.sortOrder === 'ASCENDING' ? '▲' : '▼') : '↕'}
                    </Box>
                </ButtonBase>
            ) : (
                <Box key={index} component="span">
                    {column}
                </Box>
            )
        )}
    </Box>
)

const SORT_ORDER = {
    ASCENDING: (value: string | number) => value,
    DESCENDING: (value: string | number) => (typeof value === 'number' ? -value : value),
} as const

type StateTypeKeys = keyof typeof STATE_TYPE
type ErrorTypeKeys = keyof typeof ERROR_TYPE
type SortTypeKeys = keyof typeof SORT_TYPE
type SortOrderKeys = keyof typeof SORT_ORDER

function useLocalStorageState<
    T extends string | number | StateTypeKeys[] | ErrorTypeKeys[] | SortTypeKeys[] | SortOrderKeys[],
>(key: string, defaultValue: T) {
    const [state, setState] = useState<T>(() => {
        const storedValue = localStorage.getItem(key)
        return storedValue !== null ? (JSON.parse(storedValue) as T) : defaultValue
    })

    useEffect(() => {
        localStorage.setItem(key, JSON.stringify(state))
    }, [key, state])

    return [state, setState] as const
}

export const QueryList = () => {
    // Below lg the filters wrap under the search bar, so they hide behind a "Search filters" toggle
    const filtersFitOnSearchLine = useMediaQuery(useTheme().breakpoints.up('lg'))
    const [filtersOpen, setFiltersOpen] = useState(false)
    const [allQueries, setAllQueries] = useState<QueryInfo[]>([])
    const [displayedQueries, setDisplayedQueries] = useState<QueryInfo[]>([])
    const [searchString, setSearchString] = useLocalStorageState('searchString', '' as string)
    const [stateFilters, setStateFilters] = useLocalStorageState('stateFilters', [
        'RUNNING',
        'QUEUED',
    ] as (keyof typeof STATE_TYPE)[])
    const [errorTypeFilters, setErrorTypeFilters] = useLocalStorageState('errorTypeFilters', [
        'INTERNAL_ERROR',
        'INSUFFICIENT_RESOURCES',
        'EXTERNAL',
    ] as (keyof typeof ERROR_TYPE)[])
    // Default: oldest submitted first. The keys carry a suffix so browsers that stored the previous default
    // (progress, descending) pick up the new one once; later choices persist as before.
    const [sortType, setSortType] = useLocalStorageState('sortTypeV2', 'CREATED' as keyof typeof SORT_TYPE)
    const [sortOrder, setSortOrder] = useLocalStorageState('sortOrderV2', 'ASCENDING' as keyof typeof SORT_ORDER)
    const [reorderInterval, setReorderInterval] = useLocalStorageState('reorderInterval', 5000 as number)
    const [maxQueries, setMaxQueries] = useLocalStorageState('maxQueries', 100 as number)
    const [lastReorder, setLastReorder] = useState<number>(Date.now())
    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)
    // Deliberately not persisted: every page load starts with all queries
    const [onlyMyQueries, setOnlyMyQueries] = useState(false)
    const username = useAuth().authInfo?.username
    const headerScrollRef = useRef<HTMLDivElement>(null)
    const rowsScrollRef = useRef<HTMLDivElement>(null)
    const [killTarget, setKillTarget] = useState<QueryInfo | null>(null)

    useEffect(() => {
        // Each filter change restarts the loop; a request still in flight from the previous loop is ignored.
        // One request at a time, at most every QUERY_LIST_INTERVAL_MS, none while the tab is hidden.
        let active = true
        const stop = pollCoordinator(() => getQueryListStatus(() => active), QUERY_LIST_INTERVAL_MS)
        return () => {
            active = false
            stop()
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchString, stateFilters, errorTypeFilters, sortType, sortOrder, reorderInterval, maxQueries, onlyMyQueries])

    const sortQueries = (
        incomingQueries: QueryInfo[],
        incomingSortType: keyof typeof SORT_TYPE,
        incomingSortOrder: keyof typeof SORT_ORDER
    ) => {
        incomingQueries.sort((queryA, queryB) => {
            const valueA = SORT_TYPE[incomingSortType](queryA)
            const valueB = SORT_TYPE[incomingSortType](queryB)

            if (typeof valueA === 'number' && typeof valueB === 'number') {
                // @ts-expect-error TODO fix it without using any
                return SORT_ORDER[incomingSortOrder](valueA) - SORT_ORDER[incomingSortOrder](valueB)
            }
            if (typeof valueA === 'string' && typeof valueB === 'string') {
                const comparison = valueA.localeCompare(valueB)
                return incomingSortOrder === 'ASCENDING' ? comparison : -comparison
            }

            return 0
        })
    }

    const sortAndLimitQueries = (
        incomingQueries: QueryInfo[],
        incomingSortType: keyof typeof SORT_TYPE,
        incomingSortOrder: keyof typeof SORT_ORDER,
        incomingMaxQueries: number
    ) => {
        sortQueries(incomingQueries, incomingSortType, incomingSortOrder)
        if (incomingQueries.length > incomingMaxQueries) {
            incomingQueries.splice(incomingMaxQueries, incomingQueries.length - incomingMaxQueries)
        }
    }

    const filterQueries = (
        incomingQueries: QueryInfo[],
        incomingStateFilters: (keyof typeof STATE_TYPE)[],
        incomingErrorTypeFilters: (keyof typeof ERROR_TYPE)[],
        incomingSearchString: string,
        incomingOnlyMyQueries: boolean = onlyMyQueries
    ) => {
        const stateFilteredQueries = incomingQueries.filter(
            (query) =>
                (!incomingOnlyMyQueries || query.sessionUser === username) &&
                (incomingStateFilters.some((filter) => STATE_TYPE[filter](query)) ||
                    incomingErrorTypeFilters.some((filter) => ERROR_TYPE[filter](query)))
        )

        return incomingSearchString === ''
            ? stateFilteredQueries
            : stateFilteredQueries.filter((query) => {
                  const term = incomingSearchString.toLowerCase()
                  return [
                      query.queryId,
                      getHumanReadableState(query),
                      query.queryTextPreview,
                      query.sessionUser,
                      query.sessionSource,
                      query.resourceGroupId?.join('.'),
                      query.errorCode?.name,
                      ...(query.clientTags || []),
                      query.traceToken,
                  ].some((value) => value?.toLowerCase().includes(term))
              })
    }

    // isCurrent: false once the filters that started this request have changed; its response is then stale
    // (filtered with the old settings) and must not overwrite the list the new settings already produced
    const getQueryListStatus = (isCurrent: () => boolean) => {
        setError(null)
        return queryApi().then((apiResponse: ApiResponse<QueryInfo[]>) => {
            if (!isCurrent()) {
                return
            }
            setLoading(false)
            if (apiResponse.status === 200 && apiResponse.data) {
                const queriesList = apiResponse.data
                // Derived from this same response and the app bar's stats poll: no extra requests
                updateQueryCpuShares(queriesList, useClusterStatsStore.getState().history.lastAvailableProcessors)

                const queryMap = queriesList.reduce((map: Record<string, QueryInfo>, queryInfo: QueryInfo) => {
                    map[queryInfo.queryId] = queryInfo
                    return map
                }, {})

                let updatedQueries: QueryInfo[] = []
                displayedQueries.forEach((oldQuery: QueryInfo) => {
                    if (oldQuery.queryId in queryMap) {
                        updatedQueries.push(queryMap[oldQuery.queryId])
                        delete queryMap[oldQuery.queryId]
                    }
                })

                let newQueries: QueryInfo[] = []
                for (const queryId in queryMap) {
                    if (queryMap[queryId]) {
                        newQueries.push(queryMap[queryId])
                    }
                }

                newQueries = filterQueries(newQueries, stateFilters, errorTypeFilters, searchString)

                const now: number = Date.now()

                if (reorderInterval !== 0 && now - lastReorder > reorderInterval) {
                    updatedQueries = filterQueries(updatedQueries, stateFilters, errorTypeFilters, searchString)
                    updatedQueries = updatedQueries.concat(newQueries)
                    sortQueries(updatedQueries, sortType, sortOrder)
                    setLastReorder(now)
                } else {
                    sortQueries(newQueries, sortType, sortOrder)
                    updatedQueries = updatedQueries.concat(newQueries)
                }

                if (maxQueries !== 0 && updatedQueries.length > maxQueries) {
                    updatedQueries.splice(maxQueries, updatedQueries.length - maxQueries)
                }

                setAllQueries(queriesList)
                setDisplayedQueries(updatedQueries)
            } else {
                setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
            }
        })
    }

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

    const toggleOnlyMyQueries = () => {
        const nextOnlyMyQueries = !onlyMyQueries
        const newDisplayedQueries = filterQueries(
            allQueries,
            stateFilters,
            errorTypeFilters,
            searchString,
            nextOnlyMyQueries
        )
        sortAndLimitQueries(newDisplayedQueries, sortType, sortOrder, maxQueries)
        setOnlyMyQueries(nextOnlyMyQueries)
        setDisplayedQueries(newDisplayedQueries)
    }

    const renderSearchStringTextField = () => {
        const handleChange = (newSearchString: string) => {
            setTimeout(() => {
                const newDisplayedQueries = filterQueries(allQueries, stateFilters, errorTypeFilters, newSearchString)
                sortAndLimitQueries(newDisplayedQueries, sortType, sortOrder, maxQueries)

                setSearchString(newSearchString)
                setDisplayedQueries(newDisplayedQueries)
            })
        }

        return (
            <DebouncedTextField
                label={Texts.QueryList.Filter.Search}
                placeholder={Texts.QueryList.Filter.SearchPlaceholder}
                variant="outlined"
                size="small"
                sx={{
                    '& .MuiInputBase-input': smallFormControlSx,
                    '& .MuiInputBase-input::placeholder': smallFormControlSx,
                    '& .MuiInputLabel-root': smallFormControlSx,
                }}
                value={searchString}
                onChange={(event) => handleChange(event.target.value)}
                debounceTime={500}
                fullWidth
            />
        )
    }

    const renderStateTypeSelectItem = (newStateType: keyof typeof STATE_TYPE, filterText: string) => {
        const handleClick = () => {
            setTimeout(() => {
                const newStateFilters = stateFilters.slice()
                if (stateFilters.includes(newStateType)) {
                    newStateFilters.splice(newStateFilters.indexOf(newStateType), 1)
                } else {
                    newStateFilters.push(newStateType)
                }
                const newDisplayedQueries = filterQueries(allQueries, newStateFilters, errorTypeFilters, searchString)
                sortAndLimitQueries(newDisplayedQueries, sortType, sortOrder, maxQueries)

                setStateFilters(newStateFilters)
                setDisplayedQueries(newDisplayedQueries)
            })
        }

        return (
            <MenuItem key={filterText} value={filterText} onClick={handleClick}>
                <Checkbox checked={stateFilters.includes(newStateType)} size="small" sx={{ padding: 0 }} />
                <ListItemText primary={<Typography sx={smallFormControlSx}>{filterText}</Typography>} />
            </MenuItem>
        )
    }

    const renderErrorTypeSelectItem = (newErrorType: keyof typeof ERROR_TYPE, errorTypeText: string) => {
        const handleClick = () => {
            setTimeout(() => {
                const newErrorTypeFilters = errorTypeFilters.slice()
                if (errorTypeFilters.includes(newErrorType)) {
                    newErrorTypeFilters.splice(newErrorTypeFilters.indexOf(newErrorType), 1)
                } else {
                    newErrorTypeFilters.push(newErrorType)
                }
                const newDisplayedQueries = filterQueries(allQueries, stateFilters, newErrorTypeFilters, searchString)
                sortAndLimitQueries(newDisplayedQueries, sortType, sortOrder, maxQueries)

                setErrorTypeFilters(newErrorTypeFilters)
                setDisplayedQueries(newDisplayedQueries)
            })
        }

        return (
            <MenuItem key={errorTypeText} value={errorTypeText} onClick={handleClick}>
                <Checkbox checked={errorTypeFilters.includes(newErrorType)} size="small" sx={{ padding: 0 }} />
                <ListItemText primary={<Typography sx={smallFormControlSx}>Failed - {errorTypeText}</Typography>} />
            </MenuItem>
        )
    }

    // Header click: same column flips the order; a new column starts descending (A→Z for the user column)
    const sortByColumn = (newSortType: keyof typeof SORT_TYPE) => {
        const newSortOrder: keyof typeof SORT_ORDER =
            newSortType === sortType
                ? sortOrder === 'ASCENDING'
                    ? 'DESCENDING'
                    : 'ASCENDING'
                : newSortType === 'USER'
                  ? 'ASCENDING'
                  : 'DESCENDING'
        const newDisplayedQueries = filterQueries(allQueries, stateFilters, errorTypeFilters, searchString)
        sortAndLimitQueries(newDisplayedQueries, newSortType, newSortOrder, maxQueries)
        setSortType(newSortType)
        setSortOrder(newSortOrder)
        setDisplayedQueries(newDisplayedQueries)
    }

    const renderReorderIntervalSelectItem = (newReorderInterval: number, text: string) => {
        const handleClick = () => {
            setReorderInterval(newReorderInterval)
        }

        return (
            <MenuItem value={newReorderInterval} onClick={handleClick}>
                {text}
            </MenuItem>
        )
    }

    const renderMaxQueriesSelectItem = (newMaxQueries: number, text: string) => {
        const handleClick = () => {
            setTimeout(() => {
                const newDisplayedQueries = filterQueries(allQueries, stateFilters, errorTypeFilters, searchString)
                sortAndLimitQueries(newDisplayedQueries, sortType, sortOrder, newMaxQueries)

                setMaxQueries(newMaxQueries)
                setDisplayedQueries(newDisplayedQueries)
            })
        }

        return (
            <MenuItem value={newMaxQueries} onClick={handleClick}>
                {text}
            </MenuItem>
        )
    }

    if (loading || error) {
        return (
            <Box
                sx={{ p: 2, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}
            >
                {loading ? (
                    <CircularProgress />
                ) : (
                    error && (
                        <Alert severity="error">
                            {Texts.Error.QueryListNotLoaded} - {error}
                        </Alert>
                    )
                )}
            </Box>
        )
    }

    const renderQuery = (queryInfo: QueryInfo) => (
        <Box key={queryInfo.queryId}>
            <QueryListItem queryInfo={queryInfo} onKill={setKillTarget} />
            <Divider sx={{ mb: 1 }} />
        </Box>
    )

    return (
        <>
            <Grid spacing={2} sx={{ py: 2, justifyContent: 'space-between', alignItems: 'center' }} container>
                <Grid size={{ xs: 12, lg: 4 }}>
                    {/* Wraps on very narrow screens instead of pushing the buttons off-screen */}
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, alignItems: 'center' }}>
                        {/* All (o  ) Mine: the switch toggles, and clicking a label picks that side */}
                        <Stack
                            direction="row"
                            sx={{
                                flexShrink: 0,
                                alignItems: 'center',
                                fontSize: 14,
                                opacity: username ? 1 : 0.5,
                            }}
                            title={username ? `${Texts.QueryList.Filter.MyQueries}: ${username}` : undefined}
                        >
                            <Box
                                component="span"
                                onClick={() => username && onlyMyQueries && toggleOnlyMyQueries()}
                                sx={{
                                    cursor: username ? 'pointer' : 'default',
                                    color: onlyMyQueries ? 'text.secondary' : 'text.primary',
                                    fontWeight: onlyMyQueries ? 400 : 500,
                                }}
                            >
                                {Texts.QueryList.Filter.AllQueries}
                            </Box>
                            <Switch
                                size="small"
                                disableRipple
                                checked={onlyMyQueries}
                                onChange={() => toggleOnlyMyQueries()}
                                disabled={!username}
                                slotProps={{ input: { 'aria-label': Texts.QueryList.Filter.MyQueries } }}
                            />
                            <Box
                                component="span"
                                onClick={() => username && !onlyMyQueries && toggleOnlyMyQueries()}
                                sx={{
                                    cursor: username ? 'pointer' : 'default',
                                    color: onlyMyQueries ? 'text.primary' : 'text.secondary',
                                    fontWeight: onlyMyQueries ? 500 : 400,
                                }}
                            >
                                {Texts.QueryList.Filter.MyQueries}
                            </Box>
                        </Stack>
                        <Box sx={{ flex: '1 1 120px', minWidth: 0 }}>{renderSearchStringTextField()}</Box>
                        {!filtersFitOnSearchLine && (
                            <Button
                                variant="outlined"
                                onClick={() => setFiltersOpen((open) => !open)}
                                aria-expanded={filtersOpen}
                                aria-controls="query-list-filters"
                                endIcon={<ExpandMoreIcon sx={{ transform: filtersOpen ? 'rotate(180deg)' : 'none' }} />}
                                sx={{ flexShrink: 0, whiteSpace: 'nowrap', height: 35, textTransform: 'none' }}
                            >
                                {Texts.QueryList.Filter.SearchFilters}
                            </Button>
                        )}
                    </Box>
                </Grid>
                <Grid
                    size={{ xs: 12, lg: 8 }}
                    id="query-list-filters"
                    sx={{ display: filtersFitOnSearchLine || filtersOpen ? undefined : 'none' }}
                >
                    <Stack
                        direction={{ xs: 'column', md: 'row' }}
                        sx={{
                            padding: 0,
                            gap: { xs: 2, md: 1 },
                        }}
                    >
                        <FormControl size="small" fullWidth>
                            <InputLabel sx={smallFormControlSx}>{Texts.QueryList.Filter.State}</InputLabel>
                            <Select
                                label={Texts.QueryList.Filter.State}
                                sx={smallFormControlSx}
                                MenuProps={smallDropdownMenuPropsSx}
                                value={stateFilters}
                                renderValue={(selected: string[]) => selected.join(', ')}
                                multiple
                            >
                                {renderStateTypeSelectItem('RUNNING', 'Running')}
                                {renderStateTypeSelectItem('QUEUED', 'Queued')}
                                {renderStateTypeSelectItem('FINISHED', 'Finished')}

                                {renderErrorTypeSelectItem('INTERNAL_ERROR', 'Internal error')}
                                {renderErrorTypeSelectItem('EXTERNAL', 'External')}
                                {renderErrorTypeSelectItem('INSUFFICIENT_RESOURCES', 'Insufficient resources')}
                                {renderErrorTypeSelectItem('USER_ERROR', 'User error')}
                            </Select>
                        </FormControl>
                        <FormControl size="small" fullWidth>
                            <InputLabel sx={smallFormControlSx}>{Texts.QueryList.Filter.ReorderInterval}</InputLabel>
                            <Select
                                label={Texts.QueryList.Filter.ReorderInterval}
                                sx={smallFormControlSx}
                                MenuProps={smallDropdownMenuPropsSx}
                                value={reorderInterval}
                            >
                                {renderReorderIntervalSelectItem(1000, '1s')}
                                {renderReorderIntervalSelectItem(5000, '5s')}
                                {renderReorderIntervalSelectItem(10000, '10s')}
                                {renderReorderIntervalSelectItem(30000, '30s')}
                                <Divider />
                                {renderReorderIntervalSelectItem(0, 'Off')}
                            </Select>
                        </FormControl>
                        <FormControl size="small" fullWidth>
                            <InputLabel>{Texts.QueryList.Filter.Limit}</InputLabel>
                            <Select
                                label={Texts.QueryList.Filter.Limit}
                                sx={smallFormControlSx}
                                MenuProps={smallDropdownMenuPropsSx}
                                value={maxQueries}
                            >
                                {renderMaxQueriesSelectItem(20, '20 queries')}
                                {renderMaxQueriesSelectItem(50, '50 queries')}
                                {renderMaxQueriesSelectItem(100, '100 queries')}
                                <Divider />
                                {renderMaxQueriesSelectItem(0, 'All queries')}
                            </Select>
                        </FormControl>
                    </Stack>
                </Grid>
            </Grid>
            {displayedQueries.length > 0 ? (
                <>
                    {/* The header sticks under the app bar. It sits outside the row scroller (a sideways scroller
                        would also capture vertical stickiness), so it follows the rows' horizontal scroll in code */}
                    <Box
                        sx={{
                            position: 'sticky',
                            top: APP_BAR_HEIGHT,
                            zIndex: 1,
                            bgcolor: 'background.default',
                            borderBottom: 1,
                            borderColor: 'divider',
                            mb: 0.5,
                        }}
                    >
                        <Box ref={headerScrollRef} sx={{ overflowX: 'hidden' }}>
                            <Box sx={COMPACT_TABLE_CONTENT_SX}>
                                <CompactQueryHeader sortType={sortType} sortOrder={sortOrder} onSort={sortByColumn} />
                            </Box>
                        </Box>
                    </Box>
                    <Box
                        ref={rowsScrollRef}
                        sx={COMPACT_TABLE_SCROLLER_SX}
                        onScroll={(event) => {
                            if (headerScrollRef.current) {
                                headerScrollRef.current.scrollLeft = event.currentTarget.scrollLeft
                            }
                        }}
                    >
                        <Box sx={COMPACT_TABLE_CONTENT_SX}>{displayedQueries.map(renderQuery)}</Box>
                    </Box>
                </>
            ) : (
                <Typography>
                    {allQueries.length === 0 ? Texts.QueryList.NoQueries : Texts.QueryList.NoMatchedFilterQueries}
                </Typography>
            )}
            <KillQueryDialog queryInfo={killTarget} onClose={() => setKillTarget(null)} />
        </>
    )
}
