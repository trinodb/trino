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
import { api, ApiResponse } from '../base.ts'

export interface Stats {
    runningQueries: number
    blockedQueries: number
    queuedQueries: number
    activeCoordinators: number
    activeWorkers: number
    runningDrivers: number
    totalAvailableProcessors: number
    reservedMemory: number
    totalInputRows: number
    totalInputBytes: number
    totalCpuTimeSecs: number
}

export async function statsApi(): Promise<ApiResponse<Stats>> {
    return await api.get<Stats>('/ui/api/stats')
}
