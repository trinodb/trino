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
import { Box, Card, CardContent, Grid2 as Grid, Typography } from '@mui/material'
import { Handle, Position } from '@xyflow/react'
import { STEP_NODE_WIDTH } from './layout'
import { truncateString } from '../../utils/utils.ts'
export interface IStepNodeProps {
    data: {
        label: string
        descriptor: Map<string, string>
    }
}

export const StepNode = (props: IStepNodeProps) => {
    const { label, descriptor } = props.data

    const _descriptor = truncateString(
        '(' +
            Object.entries(descriptor)
                .map(([key, value]) => key + ' = ' + String(value))
                .join(', ') +
            ')',
        35
    )

    return (
        <Box>
            <Card
                elevation={6}
                sx={{
                    border: '1px solid grey',
                    width: STEP_NODE_WIDTH,
                }}
            >
                <CardContent sx={{ p: 1, textAlign: 'center', '&:last-child': { p: 1 } }}>
                    <Grid container spacing={1}>
                        <Grid size={12}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {label}
                            </Typography>
                        </Grid>
                        <Grid size={12}>
                            <Typography variant="body2" color="text.secondary">
                                {_descriptor}
                            </Typography>
                        </Grid>
                    </Grid>
                </CardContent>
            </Card>

            <Handle style={{ opacity: 0 }} type="source" position={Position.Top} />
            <Handle style={{ opacity: 0 }} type="target" position={Position.Bottom} />
        </Box>
    )
}
