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
import { type Edge, type Node } from '@xyflow/react'
import { QueryStageStats, QueryStatusInfo } from '../../api/webapp/api'

export type LayoutDirectionType = 'BT' | 'RL'

export interface IQueryStatus {
    info: QueryStatusInfo | null
    ended: boolean
}

export interface IFlowElements {
    nodes: Node[]
    edges: Edge[]
}

export interface IPlanNodeProps {
    id: string
    name: string
    descriptor: Record<string, string>
    details: string[]
    sources: string[]
    children: IPlanNodeProps[]
}

export interface IPlanFragmentNodeInfo {
    stageId: string
    id: string
    root: string
    stageStats: QueryStageStats
    state: string
    nodes: Map<string, IPlanNodeProps>
}
