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
import { Box, Card, CardContent, Grid2 as Grid, Tooltip, Typography } from '@mui/material'
import { Handle, Position } from '@xyflow/react'
import { OPERATOR_NODE_WIDTH } from './layout'
import { truncateString } from '../../utils/utils.ts'

export interface IOperatorNodeProps {
    data: {
        label: string
        descriptor: Map<string, string>
    }
}

/**
 * Represents individual execution operators within a query plan fragment (e.g., LocalMerge, PartialSort, Aggregate).
 * These are the building blocks that form subflows inside each PlanFragmentNode, displaying specific operations
 * performed during query execution.
 *
 * Features:
 * - Displays the operator name (label) and operation parameters (descriptor)
 * - Shows truncated descriptor on the card with full details in tooltip
 * - Positioned as child nodes within PlanFragmentNode containers
 * - Connected via visible edges for sub flow connectivity
 */
export const OperatorNode = (props: IOperatorNodeProps) => {
    const { label, descriptor } = props.data

    const _descriptor =
        '(' +
        Object.entries(descriptor)
            .map(([key, value]) => key + ' = ' + String(value))
            .join(', ') +
        ')'

    return (
        <Box>
            <Card
                elevation={6}
                sx={{
                    border: '1px solid grey',
                    width: OPERATOR_NODE_WIDTH,
                }}
            >
                <CardContent sx={{ p: 1, textAlign: 'center', '&:last-child': { p: 1 } }}>
                    <Tooltip
                        placement="top"
                        title={
                            <Grid container spacing={0.5} textAlign="center" justifyContent="center">
                                <Grid size={{ xs: 12 }}>
                                    <Typography variant="subtitle2">{label}</Typography>
                                </Grid>
                                <Grid size={{ xs: 12 }}>
                                    <Typography variant="caption">{_descriptor}</Typography>
                                </Grid>
                            </Grid>
                        }
                    >
                        <Grid container spacing={1}>
                            <Grid size={{ xs: 12 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {label}
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 12 }}>
                                <Typography variant="body2" color="text.secondary">
                                    {truncateString(_descriptor, 35)}
                                </Typography>
                            </Grid>
                        </Grid>
                    </Tooltip>
                </CardContent>
            </Card>

            <Handle style={{ opacity: 0 }} type="source" position={Position.Top} />
            <Handle style={{ opacity: 0 }} type="target" position={Position.Bottom} />
        </Box>
    )
}
