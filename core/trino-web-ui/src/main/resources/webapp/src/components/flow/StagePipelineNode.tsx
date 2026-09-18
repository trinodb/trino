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
import { Box, Card, CardHeader, Typography } from '@mui/material'
import {
    STAGE_PIPELINE_NODE_WIDTH,
    STAGE_PIPELINE_NODE_PADDING_TOP,
    STAGE_OPERATOR_NODE_HEIGHT,
    STAGE_OPERATOR_NODE_GAP,
} from './layout.ts'
import { LayoutDirectionType } from './types.ts'

export interface IStagePipelineNodeProps {
    data: {
        label: string
        nrOfNodes: number
        layoutDirection: LayoutDirectionType
    }
}

/**
 * Container nodes representing a pipeline within a given stage in the stage performance flow.
 * StagePipelineNodes serve as organizational units that group multiple StageOperatorNodes
 * and coordinate operators that execute together in the same pipeline.
 *
 * Features:
 * - Contains and organizes multiple StageOperatorNode components within its boundaries
 * - Displays pipeline identifier and manages spatial layout of child operators
 * - Provides visual grouping for operators that share the same execution pipeline
 * - Positioned alongside other pipelines within the stage performance visualization
 */
export const StagePipelineNode = (props: IStagePipelineNodeProps) => {
    const { label, nrOfNodes, layoutDirection } = props.data

    return (
        <Box>
            <Card
                elevation={3}
                sx={{
                    border: '1px solid #e0e0e0',
                    borderRadius: 2,
                    width: layoutDirection === 'BT' ? STAGE_PIPELINE_NODE_WIDTH : STAGE_PIPELINE_NODE_WIDTH * nrOfNodes,
                    height:
                        layoutDirection === 'BT'
                            ? STAGE_PIPELINE_NODE_PADDING_TOP +
                              nrOfNodes * STAGE_OPERATOR_NODE_HEIGHT +
                              nrOfNodes * STAGE_OPERATOR_NODE_GAP
                            : STAGE_PIPELINE_NODE_PADDING_TOP + STAGE_OPERATOR_NODE_HEIGHT + STAGE_OPERATOR_NODE_GAP,
                    backgroundColor: 'action.hover',
                }}
            >
                <CardHeader
                    title={
                        <Typography variant="h6" sx={{ fontWeight: 600, fontSize: '1rem' }}>
                            {label}
                        </Typography>
                    }
                    sx={{ pb: 1, mr: 1 }}
                />
            </Card>
        </Box>
    )
}
