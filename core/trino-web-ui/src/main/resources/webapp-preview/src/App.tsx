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
import { HashRouter as Router, Routes, Route, Navigate } from 'react-router-dom'
import {
    Box,
    Button,
    Container,
    CssBaseline,
    Grid2 as Grid,
    Link,
    ThemeProvider,
    Typography,
    useMediaQuery,
} from '@mui/material'
import { Auth } from './components/Auth'
import { useAuth } from './components/AuthContext'
import { AuthProvider } from './components/AuthProvider'
import { RootLayout as Layout } from './components/Layout'
import { SnackbarProvider } from './components/SnackbarProvider'
import { routers } from './router.tsx'
import { useConfigStore, Theme as ThemeStore } from './store'
import { darkTheme, lightTheme } from './theme'
import trinoLogo from './assets/trino.svg'
import { QueryDetails } from './components/QueryDetails'
import { WorkerStatus } from './components/WorkerStatus'

const App = () => {
    const config = useConfigStore()
    const prefersDarkMode = useMediaQuery('(prefers-color-scheme: dark)')

    const themeToUse = () => {
        if (config.theme === ThemeStore.Auto) {
            return prefersDarkMode ? darkTheme : lightTheme
        } else if (config.theme === ThemeStore.Dark) {
            return darkTheme
        } else {
            return lightTheme
        }
    }

    return (
        <>
            <ThemeProvider theme={themeToUse()}>
                <CssBaseline />
                <SnackbarProvider>
                    <AuthProvider>
                        <Router>
                            <Screen />
                        </Router>
                    </AuthProvider>
                </SnackbarProvider>
            </ThemeProvider>
        </>
    )
}

const Screen = () => {
    const { authInfo } = useAuth()

    if (!authInfo?.authenticated) return <Auth />

    return (
        <Layout>
            <Routes>
                {routers.flatMap((router) => {
                    return [<Route {...router.routeProps} key={router.itemKey} />]
                })}
                <Route path="/workers/:nodeId" element={<WorkerStatus />} />
                <Route path="/queries/:queryId" element={<QueryDetails />} />
                <Route path="/" element={<Navigate to="/dashboard" />} />
                <Route path="*" element={<NotFound />} key={'*'} />
            </Routes>
        </Layout>
    )
}

const NotFound = () => {
    return (
        <Container maxWidth="md">
            <Grid container spacing={2}>
                <Grid size={12} spacing={{ md: 2 }}>
                    <Box component="img" sx={{ height: 140 }} alt="logo" src={trinoLogo} />
                </Grid>
                <Grid size={12} spacing={{ md: 10 }}>
                    <Typography variant="h3">404</Typography>
                    <Typography paragraph>The page you’re looking for doesn’t exist.</Typography>
                    <Button variant="contained" component={Link} href="/">
                        Back home
                    </Button>
                </Grid>
            </Grid>
        </Container>
    )
}

export default App
