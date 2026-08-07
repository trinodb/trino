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
import { ReactNode, useEffect, useState } from 'react'
import { queryStatusApi, QueryStatusInfo } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import { Texts } from '../constant.ts'
import { QueryStatusContext } from './QueryStatusContext.ts'

interface QueryStatusProviderProps {
    queryId: string | undefined
    children: ReactNode
}

export const QueryStatusProvider = ({ queryId, children }: QueryStatusProviderProps) => {
    const [queryStatusInfo, setQueryStatusInfo] = useState<QueryStatusInfo | null>(null)
    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)
    const [ended, setEnded] = useState<boolean>(false)

    useEffect(() => {
        if (!queryId) {
            return
        }

        let cancelled = false
        setQueryStatusInfo(null)
        setLoading(true)
        setError(null)
        setEnded(false)

        const runLoop = () => {
            if (cancelled) {
                return
            }
            queryStatusApi(queryId).then((apiResponse: ApiResponse<QueryStatusInfo>) => {
                if (cancelled) {
                    return
                }
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    setQueryStatusInfo(apiResponse.data)
                    setError(null)
                    const queryEnded = !!apiResponse.data.finalQueryInfo
                    setEnded(queryEnded)
                    if (!queryEnded) {
                        setTimeout(runLoop, 3000)
                    }
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                    setTimeout(runLoop, 3000)
                }
            })
        }

        runLoop()

        return () => {
            cancelled = true
        }
    }, [queryId])

    return (
        <QueryStatusContext.Provider value={{ queryStatusInfo, loading, error, ended }}>
            {children}
        </QueryStatusContext.Provider>
    )
}
