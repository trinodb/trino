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
    let hostname = new URL(url).hostname
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
    return formatDataSizeMinUnit(size, '')
}

export function formatDataSize(size: number | null): string {
    return formatDataSizeMinUnit(size, 'B')
}

function formatDataSizeMinUnit(size: number | null, minUnit: string): string {
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
    return precisionRound(size) + unit
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
