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
import { REMOTE_MERGE_NODE_HEIGHT, STEP_NODE_WIDTH } from './layout'

export const RemoteMergeNode = () => {
    return (
        <Box sx={{ py: 0, width: STEP_NODE_WIDTH, height: REMOTE_MERGE_NODE_HEIGHT }}>
            <Divider sx={{ my: 0.3 }} />
            <Handle style={{ opacity: 0 }} type="source" position={Position.Top} />
            <Handle style={{ opacity: 0 }} type="target" position={Position.Bottom} />
        </Box>
    )
}
