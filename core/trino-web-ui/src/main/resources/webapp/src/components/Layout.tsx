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
import React, { ReactNode, useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { AppBar, Box, IconButton, Stack, Toolbar, Tooltip, Typography } from '@mui/material'
import { alpha, useTheme } from '@mui/material/styles'
import { DarkModeOutlined, DensitySmallOutlined, LightModeOutlined } from '@mui/icons-material'
import FiberManualRecordIcon from '@mui/icons-material/FiberManualRecord'
import LogoutIcon from '@mui/icons-material/Logout'
import { APP_BAR_HEIGHT, Texts } from '../constant'
import trinoLogo from '../assets/trino.svg'
import { Theme as ThemeStore, useClusterStatsStore, useConfigStore } from '../store'
import { useAuth } from './AuthContext'
import { ClusterSummary } from './ClusterSummary'
import { useSnackbar } from './SnackbarContext'
import { Cluster, clusterApi } from '../api/webapp/api'
import { ApiResponse } from '../api/base'
import { pollCoordinator } from '../utils/poll'

// Compact app bar (default is 64px); the spacer in <main> uses the same sx so content starts below the bar
const headerToolbarSx = { minHeight: APP_BAR_HEIGHT, px: { xs: 1.5, sm: 2 } }

// Version and uptime change rarely; this only keeps uptime and the connection dot fresh
const CLUSTER_INFO_INTERVAL_MS = 5_000

export const RootLayout = (props: { children: React.ReactNode }) => {
    const config = useConfigStore()
    const { authInfo, logout, error } = useAuth()
    const { showSnackbar } = useSnackbar()
    const isDarkMode = useTheme().palette.mode === 'dark'
    const startClusterStatsPolling = useClusterStatsStore((state) => state.startPolling)
    const [cluster, setCluster] = useState<Cluster | null>(null)
    const [noConnection, setNoConnection] = useState<boolean | null>(null)
    const username = authInfo?.username || ''

    const HeaderStat = (props: { title: string; value?: ReactNode; valueLeading?: ReactNode }) => (
        <Box sx={{ px: 1.5, display: 'flex', alignItems: 'baseline', gap: 0.75 }}>
            <Typography
                variant="caption"
                sx={(theme) => ({
                    lineHeight: 1,
                    color: alpha(theme.palette.common.white, 0.8),
                })}
            >
                {props.title}
            </Typography>
            <Typography variant="body2" sx={{ lineHeight: 1, fontWeight: 500, textAlign: 'left' }} noWrap>
                {props.valueLeading ? (
                    <Box
                        component="span"
                        sx={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'flex-start', gap: 0.5 }}
                    >
                        {props.valueLeading}
                        <Box component="span">{props.value ?? '—'}</Box>
                    </Box>
                ) : (
                    (props.value ?? '—')
                )}
            </Typography>
        </Box>
    )

    useEffect(
        // Version, uptime and the connection dot. Cleaned up on unmount: the old loop kept running, so the dev
        // build's double mount polled twice a second.
        () => pollCoordinator(getClusterStats, CLUSTER_INFO_INTERVAL_MS),
        []
    )

    useEffect(() => {
        startClusterStatsPolling()
    }, [startClusterStatsPolling])

    useEffect(() => {
        if (error) {
            showSnackbar(error, 'error')
        }
    }, [error, showSnackbar])

    const getClusterStats = () =>
        clusterApi().then((apiResponse: ApiResponse<Cluster>) => {
            if (apiResponse.status === 200 && apiResponse.data) {
                setCluster(apiResponse.data)
                setNoConnection(false)
            } else {
                setNoConnection(true)
            }
        })

    const onLogout = () => {
        logout({ redirect: authInfo?.authType != 'form' })
    }

    const toggleTheme = () => {
        config.update((config) => (config.theme = isDarkMode ? ThemeStore.Light : ThemeStore.Dark))
    }

    return (
        <Box sx={{ display: 'flex' }}>
            <AppBar position="fixed">
                <Toolbar variant="dense" sx={headerToolbarSx}>
                    <Tooltip title={Texts.Menu.Header.Home}>
                        <IconButton component={Link} to="/" aria-label={Texts.Menu.Header.Home} sx={{ p: 0.25 }}>
                            <Box component="img" sx={{ height: 35, display: 'block' }} alt="Trino" src={trinoLogo} />
                        </IconButton>
                    </Tooltip>
                    <ClusterSummary />
                    <Stack
                        direction="row"
                        sx={{
                            flexGrow: 0,
                            alignItems: 'center',
                            mr: 1,
                            display: { xs: 'none', md: 'flex' },
                        }}
                    >
                        <HeaderStat title="Version" value={cluster?.nodeVersion?.version} />
                        <HeaderStat
                            title="Uptime"
                            value={cluster?.uptime}
                            valueLeading={
                                <FiberManualRecordIcon
                                    sx={(theme) => ({
                                        fontSize: 10,
                                        color:
                                            noConnection === true
                                                ? theme.palette.error.main
                                                : noConnection === false
                                                  ? theme.palette.success.main
                                                  : alpha(theme.palette.common.white, 0.5),
                                    })}
                                />
                            }
                        />
                    </Stack>
                    <Tooltip
                        title={config.hideQueryText ? Texts.Menu.Header.ShowQueryText : Texts.Menu.Header.HideQueryText}
                    >
                        <IconButton
                            onClick={() => config.update((config) => (config.hideQueryText = !config.hideQueryText))}
                            aria-label={Texts.Menu.Header.HideQueryText}
                            aria-pressed={config.hideQueryText}
                            // Filled while on, like a pressed toggle
                            sx={(theme) => ({
                                ml: { xs: 0.5, sm: 1 },
                                p: 0.5,
                                borderRadius: 1,
                                color: 'inherit',
                                bgcolor: config.hideQueryText ? alpha(theme.palette.common.white, 0.16) : 'transparent',
                                '&:hover': {
                                    bgcolor: alpha(theme.palette.common.white, config.hideQueryText ? 0.24 : 0.08),
                                },
                            })}
                        >
                            <DensitySmallOutlined />
                        </IconButton>
                    </Tooltip>
                    <Box sx={{ flexGrow: 0 }}>
                        <Tooltip title={isDarkMode ? Texts.Menu.Header.LightMode : Texts.Menu.Header.DarkMode}>
                            <IconButton
                                sx={{ mx: { xs: 0.5, sm: 1.5 }, p: 0.5, color: 'inherit' }}
                                onClick={toggleTheme}
                            >
                                {isDarkMode ? <DarkModeOutlined /> : <LightModeOutlined />}
                            </IconButton>
                        </Tooltip>
                    </Box>
                    <Tooltip title={username ? `${Texts.Menu.Header.Logout} (${username})` : Texts.Menu.Header.Logout}>
                        {/* span keeps the tooltip working while the button is disabled */}
                        <span>
                            <IconButton
                                onClick={onLogout}
                                disabled={authInfo?.authType === 'fixed'}
                                aria-label={Texts.Menu.Header.Logout}
                                sx={{ p: 0.5, color: 'inherit' }}
                            >
                                <LogoutIcon />
                            </IconButton>
                        </span>
                    </Tooltip>
                </Toolbar>
            </AppBar>
            {/* minWidth 0: as a flex item main would otherwise grow to its widest child and overflow the screen */}
            <Box component="main" sx={{ flexGrow: 1, minWidth: 0, p: { xs: 1.5, sm: 3 } }}>
                <Toolbar variant="dense" sx={headerToolbarSx} />
                <Box sx={{ flexGrow: 1, px: { xs: 0, sm: 2 } }}>{props.children}</Box>
            </Box>
        </Box>
    )
}
