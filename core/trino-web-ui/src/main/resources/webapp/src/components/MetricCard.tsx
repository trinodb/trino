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
import { Box, Card, Tooltip, Typography } from '@mui/material'
import { SparkLineChart } from '@mui/x-charts/SparkLineChart'
import { useTheme } from '@mui/material/styles'
import { lineClasses } from '@mui/x-charts/LineChart'
import type { Theme } from '@mui/material/styles'

interface IMetricCardProps {
    title: string
    values: number[]
    numberFormatter?: (n: number | null) => string
    tooltip?: string
}

export const MetricCard = (props: IMetricCardProps) => {
    const theme = useTheme<Theme>()
    const { title, values, numberFormatter, tooltip } = props
    const lastValue = values[values.length - 1]
    const maxValue = Math.max(...values, 1)
    const color = theme.palette.secondary.main

    // One 37px row, like the query Timeline rows: title, latest value, then the sparkline filling the rest. The
    // value has a fixed width so charts line up across tiles. The chart always keeps at least a quarter of the tile;
    // in narrow tiles the title gives way first (ellipsis; the tooltip has the full description).
    return (
        <Card variant="outlined" sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 1, height: 37 }}>
            <Tooltip placement="top-start" title={tooltip}>
                <Typography variant="body2" noWrap sx={{ flex: '0 1 9.5rem', minWidth: 0 }}>
                    {title}
                </Typography>
            </Tooltip>
            <Typography
                variant="body2"
                noWrap
                sx={{ width: '3.75rem', flexShrink: 0, textAlign: 'right', fontWeight: 'bold' }}
            >
                {numberFormatter ? numberFormatter(lastValue) : lastValue}
            </Typography>
            <Box sx={{ flex: '1 0 25%', height: 20 }}>
                <SparkLineChart
                    data={values}
                    valueFormatter={numberFormatter}
                    yAxis={{
                        min: 0,
                        max: maxValue,
                    }}
                    height={20}
                    area
                    showHighlight
                    showTooltip
                    color={color}
                    sx={{
                        [`& .${lineClasses.area}`]: {
                            fillOpacity: 0.2,
                        },
                        [`& .${lineClasses.line}`]: {
                            strokeWidth: 2,
                        },
                    }}
                />
            </Box>
        </Card>
    )
}
