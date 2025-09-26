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
import React, { ReactNode } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { Box, Divider, Grid, Tab, Tabs, Typography } from '@mui/material'
import { QueryJson } from './QueryJson'
import { QueryReferences } from './QueryReferences'
import { QueryLivePlan } from './QueryLivePlan'
import { QueryOverview } from './QueryOverview'
import { QueryStagePerformance } from './QueryStagePerformance'
import { QuerySplitsTimeline } from './QuerySplitsTimeline'

const tabValues = ['overview', 'livePlan', 'stagePerformance', 'splits', 'json', 'references'] as const
type TabValue = (typeof tabValues)[number]
const tabComponentMap: Record<TabValue, ReactNode> = {
    overview: <QueryOverview />,
    livePlan: <QueryLivePlan />,
    stagePerformance: <QueryStagePerformance />,
    splits: <QuerySplitsTimeline />,
    json: <QueryJson />,
    references: <QueryReferences />,
}
export const QueryDetails = () => {
    const { queryId } = useParams()
    const [searchParams, setSearchParams] = useSearchParams()
    const requestedTab = searchParams.get('tab')
    const tabValue: TabValue = tabValues.includes(requestedTab as TabValue) ? (requestedTab as TabValue) : 'overview'

    const handleTabChange = (_: React.SyntheticEvent, newTab: TabValue) => {
        const nextParams = new URLSearchParams(searchParams)

        if (newTab === 'overview') {
            nextParams.delete('tab')
        } else {
            nextParams.set('tab', newTab)
        }

        if (nextParams.toString() === searchParams.toString()) {
            return
        }

        setSearchParams(nextParams)
    }

    return (
        <>
            <Box sx={{ pb: 2 }}>
                <Typography variant="h4">Query details</Typography>
            </Box>

            <>
                <Grid container sx={{ pt: 2 }} alignItems="center">
                    <Grid size={{ xs: 12, lg: 4 }}>
                        <Typography variant="h6">{queryId}</Typography>
                    </Grid>
                    <Grid size={{ xs: 12, lg: 8 }}>
                        <Box display="flex" justifyContent={{ xs: 'flex-start', lg: 'flex-end' }}>
                            <Tabs value={tabValue} onChange={handleTabChange}>
                                <Tab value="overview" label="Overview" />
                                <Tab value="livePlan" label="Live plan" />
                                <Tab value="stagePerformance" label="Stage performance" />
                                <Tab value="splits" label="Splits" />
                                <Tab value="json" label="JSON" />
                                <Tab value="references" label="References" />
                            </Tabs>
                        </Box>
                    </Grid>
                </Grid>
                <Divider />

                <div>{tabComponentMap[tabValue]}</div>
            </>
        </>
    )
}
