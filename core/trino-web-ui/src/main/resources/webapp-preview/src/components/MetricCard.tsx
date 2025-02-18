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
import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import Typography from '@mui/material/Typography'
import { Grid2 as Grid } from '@mui/material'
import { SparkLineChart } from '@mui/x-charts/SparkLineChart'
import { styled } from '@mui/material/styles'

interface IMetricCardProps {
    title: string
    values: number[]
    numberFormatter?: (n: number | null) => string
    link?: string
}

const StyledLink = styled(RouterLink)(({ theme }) => ({
    textDecoration: 'none',
    color: theme.palette.info.main,
    '&:hover': {
        textDecoration: 'underline',
    },
}))

export const MetricCard = (props: IMetricCardProps) => {
    const { title, values, numberFormatter, link } = props
    const lastValue = values[values.length - 1]

    return (
        <Card variant="outlined" sx={{ minWidth: 275 }}>
            <CardContent sx={{ flex: '1 0 auto' }}>
                {link ? (
                    <StyledLink to={link}>
                        <Typography variant="h6" gutterBottom>
                            {title}
                        </Typography>
                    </StyledLink>
                ) : (
                    <Typography variant="h6" color="info" gutterBottom>
                        {title}
                    </Typography>
                )}
                <Grid container>
                    <Grid sx={{ display: 'flex', flexGrow: 1 }}>
                        <Grid
                            sx={{
                                display: 'flex',
                                justifyContent: 'center',
                                alignItems: 'center',
                                flexBasis: '0',
                                flexGrow: 1,
                            }}
                        >
                            <Typography variant="h3" color="textSecondary" sx={{ fontWeight: 'bold' }}>
                                {numberFormatter ? numberFormatter(lastValue) : lastValue}
                            </Typography>
                        </Grid>
                    </Grid>
                    <Grid sx={{ display: 'flex', flexGrow: 1 }}>
                        <Grid
                            sx={{
                                display: 'flex',
                                justifyContent: 'center',
                                alignItems: 'center',
                                flexBasis: '0',
                                flexGrow: 1,
                            }}
                        >
                            <SparkLineChart
                                data={values}
                                valueFormatter={numberFormatter}
                                height={50}
                                area
                                showHighlight
                                showTooltip
                            />
                        </Grid>
                    </Grid>
                </Grid>
            </CardContent>
        </Card>
    )
}
