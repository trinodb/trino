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
import { Link as RouterLink } from 'react-router-dom'
import { Card, CardActionArea, CardContent, Grid2 as Grid, Tooltip, Typography } from '@mui/material'
import { SparkLineChart } from '@mui/x-charts/SparkLineChart'
import { styled } from '@mui/material/styles'

interface IMetricCardProps {
    title: string
    values: number[]
    numberFormatter?: (n: number | null) => string
    link?: string
    tooltip?: string
}

const StyledLink = styled(RouterLink)(({ theme }) => ({
    textDecoration: 'none',
    color: theme.palette.info.main,
    '&:hover': {
        textDecoration: 'underline',
    },
}))

export const MetricCard = (props: IMetricCardProps) => {
    const { title, values, numberFormatter, link, tooltip } = props
    const lastValue = values[values.length - 1]
    const maxValue = Math.max(...values, 1)

    return (
        <Card variant="outlined">
            <CardActionArea disableRipple>
                <CardContent sx={{ padding: 1 }}>
                    <Grid container>
                        <Grid size={8}>
                            <Tooltip placement="top-start" title={tooltip}>
                                {link ? (
                                    <StyledLink to={link}>
                                        <Typography variant="h6">{title}</Typography>
                                    </StyledLink>
                                ) : (
                                    <Typography variant="h6" color="info">
                                        {title}
                                    </Typography>
                                )}
                            </Tooltip>
                        </Grid>
                        <Grid size={4}>
                            <Typography
                                variant="h5"
                                color="textSecondary"
                                sx={{ fontWeight: 'bold', textAlign: 'right' }}
                            >
                                {numberFormatter ? numberFormatter(lastValue) : lastValue}
                            </Typography>
                        </Grid>
                        <Grid size={12}>
                            <SparkLineChart
                                data={values}
                                valueFormatter={numberFormatter}
                                yAxis={{
                                    min: 0,
                                    max: maxValue,
                                }}
                                height={50}
                                area
                                showHighlight
                                showTooltip
                            />
                        </Grid>
                    </Grid>
                </CardContent>
            </CardActionArea>
        </Card>
    )
}
