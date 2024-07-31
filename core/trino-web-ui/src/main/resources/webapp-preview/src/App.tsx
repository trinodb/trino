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
    // TODO: update when MUI is updated to version 6
    Unstable_Grid2 as Grid,
    Link,
    ThemeProvider,
    Typography,
    useMediaQuery,
} from '@mui/material'
import { RootLayout as Layout } from './components/Layout'
import { SnackbarProvider } from './components/SnackbarProvider'
import { routers } from './router.tsx'
import { useConfigStore, Theme as ThemeStore } from './store'
import { darkTheme, lightTheme } from './theme'
import trinoLogo from './assets/trino.svg'

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
            <CssBaseline />
            <ThemeProvider theme={themeToUse()}>
                <SnackbarProvider>
                    <Router>
                        <Screen />
                    </Router>
                </SnackbarProvider>
            </ThemeProvider>
        </>
    )
}

const Screen = () => {
    return (
        <Layout>
            <Routes>
                {routers.flatMap((router) => {
                    return [<Route {...router.routeProps} key={router.itemKey} />]
                })}
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
                <Grid xs={12} md={2}>
                    <Box component="img" sx={{ height: 140 }} alt="logo" src={trinoLogo} />
                </Grid>
                <Grid xs={12} md={10}>
                    <Typography variant="h3">404</Typography>
                    <Typography paragraph>The page you’re looking for doesn’t exist.</Typography>
                    <Button variant="contained" component={Link} href="/">
                        Back Home
                    </Button>
                </Grid>
            </Grid>
        </Container>
    )
}

export default App
