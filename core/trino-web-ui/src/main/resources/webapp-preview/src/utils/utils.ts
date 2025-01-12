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

export function formatDataSize(size: number): string {
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
