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
import { Box, Divider } from '@mui/material'
import { Handle, Position } from '@xyflow/react'
import { REMOTE_EXCHANGE_NODE_HEIGHT, OPERATOR_NODE_WIDTH } from './layout'

/**
 * Represents a specific operator node that takes input from another downstream fragment in the query execution flow.
 * This node type visualizes cross-fragment data dependencies where one fragment needs to merge or consume
 * data produced by a different stage in the query plan.
 *
 * Features:
 * - Renders as a minimal divider line to show data flow connection points
 * - Positioned as the last item in the stages to indicate remote data input
 * - Uses distinguished (dotted) edges for flow connectivity
 * - Minimal height design to emphasize its role as a connection rather than processing step
 */
export const RemoteExchangeNode = () => {
    return (
        <Box sx={{ py: 0, width: OPERATOR_NODE_WIDTH, height: REMOTE_EXCHANGE_NODE_HEIGHT }}>
            <Divider sx={{ my: 0.3 }} />
            <Handle style={{ opacity: 0 }} type="source" position={Position.Top} />
            <Handle style={{ opacity: 0 }} type="target" position={Position.Bottom} />
        </Box>
    )
}
