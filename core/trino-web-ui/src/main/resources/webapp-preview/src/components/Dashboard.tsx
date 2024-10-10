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
import { useEffect, useState } from 'react';
import Typography from '@mui/material/Typography';
import {
    Box,
    Grid2 as Grid,
    CircularProgress
} from "@mui/material";
import { statsApi } from "../api/webapp/api.ts";
import { Texts } from "../constant.ts";
import { useSnackbar } from "./SnackbarContext.ts";
import { CodeBlock } from './CodeBlock.tsx';

interface ClusterStatsType
{
    runningQueries: number;
    blockedQueries: number;
    queuedQueries: number;
    activeCoordinators: number;
    activeWorkers: number;
    runningDrivers: number;
    totalAvailableProcessors: number;
    reservedMemory: number;
    totalInputRows: number;
    totalInputBytes: number;
    totalCpuTimeSecs: number;
}

export const Dashboard = () => {
    const { showSnackbar } = useSnackbar();
    const [clusterStats, setClusterStats] = useState<ClusterStatsType>();
    const [loading, setLoading] = useState<boolean>(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        getClusterStats();
        const intervalId = setInterval(getClusterStats, 5000);
        return () => clearInterval(intervalId);
    }, []);

    useEffect(() => {
        if (error) {
            showSnackbar(error, 'error');
        }
    }, [error, showSnackbar])

    const getClusterStats = () => {
        setError(null);
        setLoading(true);
        statsApi().then((apiResponse) => {
            if (apiResponse.ok) {
                setError(null);
                setClusterStats(apiResponse.result);
            } else {
                setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.statusText}`);
            }
            setLoading(false);
        })
    };

    return (
        <>
            <Box sx={{ pb: 2 }}>
                <Typography variant="h4">
                    Dashboard
                </Typography>
            </Box>
            <Typography variant="h6">
                {Texts.Api.Stats.Name} (<code>/ui/api/stats</code>)
            </Typography>
            {loading ?
                <Grid container spacing={2} alignItems="center">
                    <Grid>
                        <CircularProgress size={28} />
                    </Grid>
                    <Grid>
                        <Typography>{Texts.Api.FetchingData}</Typography>
                    </Grid>
                </Grid>
                : <CodeBlock code={JSON.stringify(clusterStats, null, 2)} language="json" />
            }
        </>
    );
}
