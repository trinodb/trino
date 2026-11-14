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
import type { ChipProps } from '@mui/material/Chip'
import { QueryInfoBase } from '../api/webapp/api.ts'

export const getHumanReadableState = (queryInfoBase: QueryInfoBase) => {
    if (queryInfoBase.state === 'RUNNING') {
        let title = 'RUNNING'

        if (
            queryInfoBase.scheduled &&
            queryInfoBase.queryStats.totalDrivers > 0 &&
            queryInfoBase.queryStats.runningDrivers >= 0
        ) {
            if (queryInfoBase.queryStats.fullyBlocked) {
                title = 'BLOCKED'

                if (queryInfoBase.queryStats.blockedReasons?.length > 0) {
                    title += ' (' + queryInfoBase.queryStats.blockedReasons.join(', ') + ')'
                }
            }

            if (queryInfoBase.memoryPool === 'reserved') {
                title += ' (RESERVED)'
            }

            return title
        }
    }

    if (queryInfoBase.state === 'FAILED') {
        let errorMsg = ''
        switch (queryInfoBase.errorType) {
            case 'USER_ERROR':
                errorMsg = 'USER ERROR'
                if (queryInfoBase.errorCode.name === 'USER_CANCELED') {
                    errorMsg = 'USER CANCELED'
                }
                break
            case 'INTERNAL_ERROR':
                errorMsg = 'INTERNAL ERROR'
                break
            case 'INSUFFICIENT_RESOURCES':
                errorMsg = 'INSUFFICIENT RESOURCES'
                break
            case 'EXTERNAL':
                errorMsg = 'EXTERNAL ERROR'
                break
        }
        if (queryInfoBase.errorCode && queryInfoBase.errorCode.name) {
            errorMsg += ` - ${queryInfoBase.errorCode.name}`
        }
        return errorMsg
    }

    return queryInfoBase.state
}

const STATE_COLOR_MAP: Record<string, ChipProps['color']> = {
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

export const getQueryStateColor = (query: QueryInfoBase): ChipProps['color'] => {
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

// Finished and failed queries can no longer be killed
export const isKillable = (query: QueryInfoBase) => query.state !== 'FINISHED' && query.state !== 'FAILED'

// Short status for the compact list's Status column, in lifecycle order (the order the column sorts by). The
// error itself is shown on its own line under failed rows.
const COMPACT_STATUSES = [
    'Queued',
    'Waiting',
    'Dispatching',
    'Planning',
    'Starting',
    'Running',
    'Blocked',
    'Finishing',
    'Finished',
    'Failed',
    'Canceled',
] as const

export const getCompactStatusLabel = (query: QueryInfoBase): (typeof COMPACT_STATUSES)[number] => {
    switch (query.state) {
        case 'QUEUED':
            return 'Queued'
        case 'WAITING_FOR_RESOURCES':
            return 'Waiting'
        case 'DISPATCHING':
            return 'Dispatching'
        case 'PLANNING':
            return 'Planning'
        case 'STARTING':
            return 'Starting'
        case 'RUNNING':
            return query.queryStats.fullyBlocked ? 'Blocked' : 'Running'
        case 'FINISHING':
            return 'Finishing'
        case 'FINISHED':
            return 'Finished'
        default:
            return query.errorCode?.name === 'USER_CANCELED' ? 'Canceled' : 'Failed'
    }
}

// Lifecycle position, then progress, so running queries sort by how far along they are
export const getCompactStatusRank = (query: QueryInfoBase) =>
    COMPACT_STATUSES.indexOf(getCompactStatusLabel(query)) * 1000 +
    (query.state === 'RUNNING' ? getQueryProgressPercentage(query) : 0)

export const getQueryProgressPercentage = (query: QueryInfoBase) => {
    if (query.state !== 'RUNNING') {
        return 100
    }

    const progress = query.queryStats.progressPercentage || 0
    return Math.round(progress)
}

// Sparkline-related functions
// ===========================

// display at most 5 minutes worth of data on the sparklines
export const MAX_HISTORY = 60 * 5
// alpha param of exponentially weighted moving average. picked arbitrarily - lower values means more smoothness
const MOVING_AVERAGE_ALPHA = 0.2

export function addToHistory(value: number, valuesArray: number[]): number[] {
    if (valuesArray.length === 0) {
        return valuesArray.concat([value])
    }
    return valuesArray.concat([value]).slice(Math.max(valuesArray.length - MAX_HISTORY, 0))
}

export function addExponentiallyWeightedToHistory(value: number, valuesArray: number[]): number[] {
    if (valuesArray.length === 0) {
        return valuesArray.concat([value])
    }

    let movingAverage = value * MOVING_AVERAGE_ALPHA + valuesArray[valuesArray.length - 1] * (1 - MOVING_AVERAGE_ALPHA)
    if (value < 1) {
        movingAverage = 0
    }

    return valuesArray.concat([movingAverage]).slice(Math.max(valuesArray.length - MAX_HISTORY, 0))
}

// Utility functions
// =================

export function truncateString(inputString: string, length: number): string {
    if (inputString && inputString.length > length) {
        return inputString.substring(0, length) + '...'
    }

    return inputString
}

export function getStageNumber(stageId: string): number {
    return Number.parseInt(stageId.slice(stageId.indexOf('.') + 1, stageId.length))
}

export function getTaskIdSuffix(taskId: string): string {
    return taskId.slice(taskId.indexOf('.') + 1, taskId.length)
}

export function getTaskNumber(taskId: string): number {
    return Number.parseInt(getTaskIdSuffix(getTaskIdSuffix(taskId)))
}

export function getHostname(url: string): string {
    let hostname
    try {
        hostname = new URL(url).hostname
    } catch {
        hostname = /^[a-z][a-z0-9+.-]*:\/\/(?:[^@/?#]*@)?(\[[^\]]*\]|[^:/?#]*)/i.exec(url)?.[1] ?? url
    }
    if (hostname.charAt(0) === '[' && hostname.charAt(hostname.length - 1) === ']') {
        hostname = hostname.substring(1, hostname.length - 1)
    }
    return hostname
}

export function computeRate(count: number, ms: number): number {
    if (ms === 0) {
        return 0
    }
    return (count / ms) * 1000.0
}

export function precisionRound(n: number | null): string {
    if (n === null) {
        return ''
    }

    if (n === undefined) {
        return 'n/a'
    }
    if (n < 10) {
        return n.toFixed(2)
    }
    if (n < 100) {
        return n.toFixed(1)
    }
    return Math.round(n).toString()
}

// Whole units instead of fractions of the largest one: 80ms, 4s, 45s, 12m 30s, 3h 46m, 2d 3h
export function formatDurationClock(duration: number | null): string {
    if (duration == null) {
        return ''
    }
    if (duration < 1000) {
        return `${Math.round(duration)}ms`
    }
    const seconds = Math.floor(duration / 1000)
    if (seconds < 60) {
        return `${seconds}s`
    }
    const minutes = Math.floor(seconds / 60)
    const hours = Math.floor(minutes / 60)
    const days = Math.floor(hours / 24)
    if (minutes < 60) {
        return `${minutes}m ${seconds % 60}s`
    }
    if (hours < 24) {
        return `${hours}h ${minutes % 60}m`
    }
    return `${days}d ${hours % 24}h`
}

export function formatDuration(duration: number | null): string {
    if (duration == null) {
        return ''
    }

    let unit = 'ms'
    if (duration > 1000) {
        duration /= 1000
        unit = 's'
    }
    if (unit === 's' && duration > 60) {
        duration /= 60
        unit = 'm'
    }
    if (unit === 'm' && duration > 60) {
        duration /= 60
        unit = 'h'
    }
    if (unit === 'h' && duration > 24) {
        duration /= 24
        unit = 'd'
    }
    if (unit === 'd' && duration > 7) {
        duration /= 7
        unit = 'w'
    }
    return precisionRound(duration) + unit
}

export function formatRows(count: number): string {
    if (count === 1) {
        return '1 row'
    }

    return formatCount(count) + ' rows'
}

export function formatCount(count: number | null): string {
    if (count === null) {
        return ''
    }

    let unit = ''
    if (count > 1000) {
        count /= 1000
        unit = 'K'
    }
    if (count > 1000) {
        count /= 1000
        unit = 'M'
    }
    if (count > 1000) {
        count /= 1000
        unit = 'B'
    }
    if (count > 1000) {
        count /= 1000
        unit = 'T'
    }
    if (count > 1000) {
        count /= 1000
        unit = 'Q'
    }
    return precisionRound(count) + unit
}

export function formatDataSizeBytes(size: number | null): string {
    return formatDataSizeMinUnit(size, '', false)
}

// Like formatDataSizeBytes but rounded to a whole number of the chosen unit, e.g. 4.11G -> 4G
export function formatDataSizeBytesWhole(size: number | null): string {
    return formatDataSizeMinUnit(size, '', true)
}

export function formatDataSize(size: number | null): string {
    return formatDataSizeMinUnit(size, 'B', false)
}

function formatDataSizeMinUnit(size: number | null, minUnit: string, wholeNumber: boolean): string {
    if (size === null) {
        return ''
    }

    let unit = minUnit
    if (size === 0) {
        return '0' + unit
    }
    if (size >= 1024) {
        size /= 1024
        unit = 'K' + minUnit
    }
    if (size >= 1024) {
        size /= 1024
        unit = 'M' + minUnit
    }
    if (size >= 1024) {
        size /= 1024
        unit = 'G' + minUnit
    }
    if (size >= 1024) {
        size /= 1024
        unit = 'T' + minUnit
    }
    if (size >= 1024) {
        size /= 1024
        unit = 'P' + minUnit
    }
    return (wholeNumber ? Math.round(size).toString() : precisionRound(size)) + unit
}

export function parseDataSize(value: string): number | null {
    const DATA_SIZE_PATTERN = /^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$/
    const match = DATA_SIZE_PATTERN.exec(value)
    if (match === null) {
        return null
    }
    const number = parseFloat(match[1])
    switch (match[2]) {
        case 'B':
            return number
        case 'kB':
            return number * Math.pow(2, 10)
        case 'MB':
            return number * Math.pow(2, 20)
        case 'GB':
            return number * Math.pow(2, 30)
        case 'TB':
            return number * Math.pow(2, 40)
        case 'PB':
            return number * Math.pow(2, 50)
        default:
            return null
    }
}

export function parseAndFormatDataSize(value: string): string {
    const parsed = parseDataSize(value)

    if (parsed == null) {
        return ''
    }

    return formatDataSize(parsed)
}

export function parseDuration(value: string): number | null {
    const DURATION_PATTERN = /^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$/

    const match = DURATION_PATTERN.exec(value)
    if (match === null) {
        return null
    }
    const number = parseFloat(match[1])
    switch (match[2]) {
        case 'ns':
            return number / 1000000.0
        case 'us':
            return number / 1000.0
        case 'ms':
            return number
        case 's':
            return number * 1000
        case 'm':
            return number * 1000 * 60
        case 'h':
            return number * 1000 * 60 * 60
        case 'd':
            return number * 1000 * 60 * 60 * 24
        default:
            return null
    }
}

export function formatShortTime(date: Date): string {
    return date.toLocaleTimeString([], { timeStyle: 'short' })
}

export function formatShortDateTime(date: Date): string {
    const year = date.getFullYear()
    const month = '' + (date.getMonth() + 1)
    const dayOfMonth = '' + date.getDate()
    return (
        year +
        '-' +
        (month[1] ? month : '0' + month[0]) +
        '-' +
        (dayOfMonth[1] ? dayOfMonth : '0' + dayOfMonth[0]) +
        ' ' +
        formatShortTime(date)
    )
}
