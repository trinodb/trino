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
import React, { ReactNode, useState } from 'react'
import { useLocation, useParams } from 'react-router-dom'
import { Alert, Box, Divider, Grid2 as Grid, Tabs, Tab, Typography } from '@mui/material'
import { QueryJson } from './QueryJson'
import { QueryReferences } from './QueryReferences'
import { QueryLivePlan } from './QueryLivePlan'
import { QueryOverview } from './QueryOverview'
import { QueryStagePerformance } from './QueryStagePerformance'
import { Texts } from '../constant.ts'

const tabValues = ['overview', 'livePlan', 'stagePerformance', 'splits', 'json', 'references'] as const
type TabValue = (typeof tabValues)[number]
const tabComponentMap: Record<TabValue, ReactNode> = {
    overview: <QueryOverview />,
    livePlan: <QueryLivePlan />,
    stagePerformance: <QueryStagePerformance />,
    splits: <Alert severity="error">{Texts.Error.NotImplemented}</Alert>,
    json: <QueryJson />,
    references: <QueryReferences />,
}
export const QueryDetails = () => {
    const { queryId } = useParams()
    const location = useLocation()
    const queryParams = new URLSearchParams(location.search)
    const requestedTab = queryParams.get('tab')
    const [tabValue, setTabValue] = useState<TabValue>(
        tabValues.includes(requestedTab as TabValue) ? (requestedTab as TabValue) : 'overview'
    )

    const handleTabChange = (_: React.SyntheticEvent, newTab: TabValue) => {
        setTabValue(newTab)
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
                                <Tab value="splits" label="Splits" disabled />
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
