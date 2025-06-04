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
import { useEffect, useState } from 'react'
import {
    Alert,
    Box,
    Checkbox,
    CircularProgress,
    Divider,
    FormControl,
    Grid2 as Grid,
    MenuItem,
    InputLabel,
    ListItemText,
    Select,
    Stack,
    Typography,
} from '@mui/material'
import { DebouncedTextField } from './DebouncedTextField.tsx'
import { QueryListItem } from './QueryListItem.tsx'
import { ApiResponse } from '../api/base.ts'
import { Texts } from '../constant.ts'
import { QueryInfo, queryApi } from '../api/webapp/api.ts'
import { getHumanReadableState, parseDataSize, parseDuration } from '../utils/utils.ts'

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

const SORT_TYPE = {
    CREATED: (queryInfo: QueryInfo) => Date.parse(queryInfo.queryStats.createTime),
    ELAPSED: (queryInfo: QueryInfo) => parseDuration(queryInfo.queryStats.elapsedTime),
    EXECUTION: (queryInfo: QueryInfo) => parseDuration(queryInfo.queryStats.executionTime),
    CPU: (queryInfo: QueryInfo) => parseDuration(queryInfo.queryStats.totalCpuTime),
    CUMULATIVE_MEMORY: (queryInfo: QueryInfo) => queryInfo.queryStats.cumulativeUserMemory,
    CURRENT_MEMORY: (queryInfo: QueryInfo) => parseDataSize(queryInfo.queryStats.userMemoryReservation),
} as const

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
    const [sortType, setSortType] = useLocalStorageState('sortType', 'CREATED' as keyof typeof SORT_TYPE)
    const [sortOrder, setSortOrder] = useLocalStorageState('sortOrder', 'DESCENDING' as keyof typeof SORT_ORDER)
    const [reorderInterval, setReorderInterval] = useLocalStorageState('reorderInterval', 5000 as number)
    const [maxQueries, setMaxQueries] = useLocalStorageState('maxQueries', 100 as number)
    const [lastReorder, setLastReorder] = useState<number>(Date.now())
    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        let timeoutId: number
        const runLoop = () => {
            getQueryListStatus()
            timeoutId = setTimeout(runLoop, 1000)
        }
        runLoop()

        return () => clearTimeout(timeoutId)
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchString, stateFilters, errorTypeFilters, sortType, sortOrder, reorderInterval, maxQueries])

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
        incomingSearchString: string
    ) => {
        const stateFilteredQueries = incomingQueries.filter(
            (query) =>
                incomingStateFilters.some((filter) => STATE_TYPE[filter](query)) ||
                incomingErrorTypeFilters.some((filter) => ERROR_TYPE[filter](query))
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
                  ].some((value) => value?.toLowerCase().includes(term))
              })
    }

    const getQueryListStatus = () => {
        setError(null)
        queryApi().then((apiResponse: ApiResponse<QueryInfo[]>) => {
            setLoading(false)
            if (apiResponse.status === 200 && apiResponse.data) {
                const queriesList = apiResponse.data

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
        PaperProps: {
            sx: {
                '& .MuiMenuItem-root': smallFormControlSx,
            },
        },
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
            <MenuItem key={filterText} value={filterText}>
                <Checkbox
                    checked={stateFilters.includes(newStateType)}
                    size="small"
                    sx={{ padding: 0 }}
                    onClick={handleClick}
                />
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
            <MenuItem key={errorTypeText} value={errorTypeText}>
                <Checkbox
                    checked={errorTypeFilters.includes(newErrorType)}
                    size="small"
                    sx={{ padding: 0 }}
                    onClick={handleClick}
                />
                <ListItemText primary={<Typography sx={smallFormControlSx}>Failed - {errorTypeText}</Typography>} />
            </MenuItem>
        )
    }

    const renderSortTypeSelectItem = (newSortType: keyof typeof SORT_TYPE, text: string) => {
        const handleClick = () => {
            setTimeout(() => {
                const newDisplayedQueries = filterQueries(allQueries, stateFilters, errorTypeFilters, searchString)
                sortAndLimitQueries(newDisplayedQueries, newSortType, sortOrder, maxQueries)

                setSortType(newSortType)
                setDisplayedQueries(newDisplayedQueries)
            })
        }

        return (
            <MenuItem value={newSortType} onClick={handleClick}>
                {text}
            </MenuItem>
        )
    }

    const renderSortOrderSelectItem = (newSortOrder: keyof typeof SORT_ORDER, text: string) => {
        const handleClick = () => {
            setTimeout(() => {
                const newDisplayedQueries = filterQueries(allQueries, stateFilters, errorTypeFilters, searchString)
                sortAndLimitQueries(newDisplayedQueries, sortType, newSortOrder, maxQueries)

                setSortOrder(newSortOrder)
                setDisplayedQueries(newDisplayedQueries)
            })
        }

        return (
            <MenuItem value={newSortOrder} onClick={handleClick}>
                {text}
            </MenuItem>
        )
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
            <Box sx={{ p: 2 }} display="flex" flexDirection="column" alignItems="center" justifyContent="center">
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

    return (
        <>
            <Grid spacing={2} sx={{ py: 2 }} justifyContent="space-between" alignItems="center" container>
                <Grid size={{ xs: 12, lg: 4 }}>
                    <Box>{renderSearchStringTextField()}</Box>
                </Grid>
                <Grid size={{ xs: 12, lg: 8 }}>
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
                            <InputLabel sx={smallFormControlSx}>{Texts.QueryList.Filter.SortBy}</InputLabel>
                            <Select
                                label={Texts.QueryList.Filter.SortBy}
                                sx={smallFormControlSx}
                                MenuProps={smallDropdownMenuPropsSx}
                                value={sortType}
                            >
                                {renderSortTypeSelectItem('CREATED', 'Creation time')}
                                {renderSortTypeSelectItem('ELAPSED', 'Elapsed time')}
                                {renderSortTypeSelectItem('CPU', 'CPU time')}
                                {renderSortTypeSelectItem('EXECUTION', 'Execution time')}
                                {renderSortTypeSelectItem('CURRENT_MEMORY', 'Current memory')}
                                {renderSortTypeSelectItem('CUMULATIVE_MEMORY', 'Cumulative user memory')}
                            </Select>
                        </FormControl>
                        <FormControl size="small" fullWidth>
                            <InputLabel sx={smallFormControlSx}>{Texts.QueryList.Filter.Ordering}</InputLabel>
                            <Select
                                label={Texts.QueryList.Filter.Ordering}
                                sx={smallFormControlSx}
                                MenuProps={smallDropdownMenuPropsSx}
                                value={sortOrder}
                            >
                                {renderSortOrderSelectItem('ASCENDING', 'Ascending')}
                                {renderSortOrderSelectItem('DESCENDING', 'Descending')}
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
            <Stack direction="column">
                {displayedQueries.length > 0 ? (
                    displayedQueries.map((queryInfo: QueryInfo) => (
                        <Box key={queryInfo.queryId}>
                            <QueryListItem queryInfo={queryInfo} />
                            <Divider sx={{ mb: 1 }} />
                        </Box>
                    ))
                ) : (
                    <Typography>
                        {allQueries.length === 0 ? Texts.QueryList.NoQueries : Texts.QueryList.NoMatchedFilterQueries}
                    </Typography>
                )}
            </Stack>
        </>
    )
}
