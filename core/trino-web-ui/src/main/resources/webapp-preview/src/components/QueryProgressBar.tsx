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
import { LinearProgressWithLabel, LinearProgressWithLabelProps } from './LinearProgressWithLabel'
import { QueryInfoBase } from '../api/webapp/api.ts'
import { getHumanReadableState } from '../utils/utils.ts'

export interface QueryProgressBarProps {
    queryInfoBase: QueryInfoBase
}

export const QueryProgressBar = (props: QueryProgressBarProps) => {
    const { queryInfoBase } = props

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

    const getQueryStateColor = (query: QueryInfoBase): LinearProgressWithLabelProps['color'] => {
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

    const getProgressBarPercentage = (query: QueryInfoBase) => {
        if (query.state !== 'RUNNING') {
            return 100
        }

        const progress = query.queryStats.progressPercentage || 0
        return Math.round(progress)
    }

    const getProgressBarTitle = (query: QueryInfoBase) => {
        return getHumanReadableState(query)
    }

    return (
        <LinearProgressWithLabel
            value={getProgressBarPercentage(queryInfoBase)}
            title={getProgressBarTitle(queryInfoBase)}
            color={getQueryStateColor(queryInfoBase)}
        />
    )
}
