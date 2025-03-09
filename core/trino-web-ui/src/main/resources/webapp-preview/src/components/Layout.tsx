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
import { useLocation, Link } from 'react-router-dom'
import {
    Avatar,
    AppBar,
    Box,
    Container,
    IconButton,
    List,
    ListItem,
    ListItemButton,
    ListItemIcon,
    ListItemText,
    Menu,
    MenuItem,
    Drawer as MuiDrawer,
    Toolbar,
    Tooltip,
    Typography,
} from '@mui/material'
import { CSSObject, Theme, styled } from '@mui/material/styles'
import { DarkModeOutlined, LightModeOutlined, SettingsBrightnessOutlined } from '@mui/icons-material'
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft'
import ChevronRightIcon from '@mui/icons-material/ChevronRight'
import LogoutIcon from '@mui/icons-material/Logout'
import { Texts } from '../constant'
import trinoLogo from '../assets/trino.svg'
import { RouterItem, routers, routersMapper } from '../router.tsx'
import { Theme as ThemeStore, useConfigStore } from '../store'
import { useAuth } from './AuthContext'
import { useSnackbar } from './SnackbarContext'

interface settingsMenuItem {
    key: string
    caption: string
    icon?: ReactNode
    divider?: boolean
    onClick?: () => void
    disabled?: boolean
}

const openedDrawer = (theme: Theme, width: number): CSSObject => ({
    width: width,
    transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.enteringScreen,
    }),
    overflowX: 'hidden',
})

const closedDrawer = (theme: Theme): CSSObject => ({
    transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
    }),
    overflowX: 'hidden',
    width: `calc(${theme.spacing(7)} + 1px)`,
    [theme.breakpoints.up('sm')]: {
        width: `calc(${theme.spacing(8)} + 1px)`,
    },
})

const DrawerFooter = styled('div')(({ theme }) => ({
    display: 'flex',
    flexGrow: 0,
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: theme.spacing(0, 1.5),
}))

// @ts-expect-error TS2339
const Drawer = styled(MuiDrawer)(({ theme, open, width }) => ({
    width: width,
    flexShrink: 0,
    whiteSpace: 'nowrap',
    boxSizing: 'border-box',
    ...(open && {
        ...openedDrawer(theme, width),
        '& .MuiDrawer-paper': openedDrawer(theme, width),
    }),
    ...(!open && {
        ...closedDrawer(theme),
        '& .MuiDrawer-paper': closedDrawer(theme),
    }),
}))

export const RootLayout = (props: { children: React.ReactNode }) => {
    const config = useConfigStore()
    const { authInfo, logout, error } = useAuth()
    const { showSnackbar } = useSnackbar()
    const location = useLocation()
    const [drawerOpen, setDrawerOpen] = useState<boolean>(true)
    const [selectedDrawerItemKey, setSelectedDrawerItemKey] = useState(
        location.pathname.substring(location.pathname.lastIndexOf('/') + 1)
    )
    const [anchorElTheme, setAnchorElTheme] = useState<Element | null>(null)
    const [anchorElUser, setAnchorElUser] = useState<Element | null>(null)
    const username = authInfo?.username || ''
    const userInitials = username?.charAt(0).toUpperCase()

    useEffect(() => {
        const router = routersMapper[location.pathname]
        if (router && router.itemKey != null && selectedDrawerItemKey !== router.itemKey) {
            setSelectedDrawerItemKey(router.itemKey)
        }
    }, [location, selectedDrawerItemKey])

    useEffect(() => {
        if (error) {
            showSnackbar(error, 'error')
        }
    }, [error, showSnackbar])

    const onLogout = () => {
        logout({ redirect: authInfo?.authType != 'form' })
    }

    const openUserMenu = (event: React.MouseEvent) => {
        setAnchorElUser(event.currentTarget)
    }

    const closeUserMenu = () => {
        setAnchorElUser(null)
    }

    const openThemeMenu = (event: React.MouseEvent) => {
        setAnchorElTheme(event.currentTarget)
    }

    const closeThemeMenu = () => {
        setAnchorElTheme(null)
    }

    const setTheme = (theme: ThemeStore) => {
        config.update((config) => (config.theme = theme))
    }

    const toggleDrawer = () => {
        drawerOpen ? setDrawerOpen(false) : setDrawerOpen(true)
    }

    const settingsMenuItems: settingsMenuItem[] = [
        {
            key: 'username',
            caption: username,
            divider: true,
        },
        {
            key: 'logout',
            caption: Texts.Menu.Header.Logout,
            onClick: () => {
                closeUserMenu()
                onLogout()
            },
            icon: <LogoutIcon />,
            disabled: authInfo?.authType === 'fixed',
        },
    ]

    return (
        <Box sx={{ display: 'flex' }}>
            <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
                <Toolbar>
                    <Box component="img" sx={{ height: 50 }} alt="logo" src={trinoLogo} />
                    <Typography
                        variant="h6"
                        noWrap
                        component="a"
                        href="/"
                        sx={{
                            mx: 2,
                            display: 'flex',
                            flexGrow: 1,
                            fontFamily: 'roboto',
                            fontWeight: 400,
                            color: 'inherit',
                            textDecoration: 'none',
                        }}
                    >
                        Trino
                    </Typography>
                    <Box sx={{ flexGrow: 0 }}>
                        <Tooltip title={Texts.Menu.Header.Themes}>
                            <IconButton sx={{ mx: 2, color: 'inherit' }} onClick={openThemeMenu}>
                                {config.theme === ThemeStore.Auto ? (
                                    <SettingsBrightnessOutlined />
                                ) : config.theme === ThemeStore.Light ? (
                                    <LightModeOutlined />
                                ) : config.theme === ThemeStore.Dark ? (
                                    <DarkModeOutlined />
                                ) : null}
                            </IconButton>
                        </Tooltip>
                        <Menu
                            sx={{ mt: '45px' }}
                            id="menu-appbar"
                            anchorEl={anchorElTheme}
                            anchorOrigin={{
                                vertical: 'top',
                                horizontal: 'right',
                            }}
                            keepMounted
                            transformOrigin={{
                                vertical: 'top',
                                horizontal: 'right',
                            }}
                            open={Boolean(anchorElTheme)}
                            onClose={closeThemeMenu}
                        >
                            {Object.keys(ThemeStore).map((key) => {
                                const value = ThemeStore[key as keyof typeof ThemeStore]
                                return (
                                    <MenuItem
                                        key={key}
                                        onClick={() => setTheme(value)}
                                        selected={value === config.theme}
                                    >
                                        <ListItemIcon>
                                            {value === ThemeStore.Auto ? (
                                                <SettingsBrightnessOutlined />
                                            ) : value === ThemeStore.Light ? (
                                                <LightModeOutlined />
                                            ) : value === ThemeStore.Dark ? (
                                                <DarkModeOutlined />
                                            ) : null}
                                        </ListItemIcon>
                                        <Typography textAlign="center">{key}</Typography>
                                    </MenuItem>
                                )
                            })}
                        </Menu>
                    </Box>
                    <Box sx={{ flexGrow: 0 }}>
                        <Tooltip title={Texts.Menu.Header.Settings}>
                            <IconButton onClick={openUserMenu} sx={{ p: 0 }}>
                                <Avatar src={config.avatar}>{userInitials}</Avatar>
                            </IconButton>
                        </Tooltip>
                        <Menu
                            sx={{ mt: '45px' }}
                            id="menu-appbar"
                            anchorEl={anchorElUser}
                            anchorOrigin={{
                                vertical: 'top',
                                horizontal: 'right',
                            }}
                            keepMounted
                            transformOrigin={{
                                vertical: 'top',
                                horizontal: 'right',
                            }}
                            open={Boolean(anchorElUser)}
                            onClose={closeUserMenu}
                        >
                            {settingsMenuItems.map((settingsMenuItem) => (
                                <MenuItem
                                    sx={{ justifyContent: 'flex-start' }}
                                    key={settingsMenuItem.key}
                                    divider={settingsMenuItem.divider}
                                    onClick={settingsMenuItem.onClick}
                                    disabled={settingsMenuItem.disabled}
                                >
                                    {settingsMenuItem.icon ? (
                                        <ListItemIcon>{settingsMenuItem.icon}</ListItemIcon>
                                    ) : (
                                        <div />
                                    )}
                                    <Typography>{settingsMenuItem.caption}</Typography>
                                </MenuItem>
                            ))}
                        </Menu>
                    </Box>
                </Toolbar>
            </AppBar>
            {/*@ts-expect-error TS2322*/}
            <Drawer variant="permanent" open={drawerOpen} width={240}>
                <Toolbar />
                <Box sx={{ overflowX: 'hidden', flexGrow: 1 }}>
                    <List>
                        {routers.map((routerItem: RouterItem) =>
                            routerItem.hidden ? null : (
                                <ListItem key={routerItem.text} disablePadding>
                                    {/*@ts-expect-error TS2769*/}
                                    <ListItemButton
                                        key={routerItem.itemKey}
                                        component={Link}
                                        to={routerItem.routeProps.path}
                                        selected={routerItem.itemKey === selectedDrawerItemKey}
                                    >
                                        <ListItemIcon>{routerItem.icon}</ListItemIcon>
                                        <ListItemText primary={routerItem.text} />
                                    </ListItemButton>
                                </ListItem>
                            )
                        )}
                    </List>
                </Box>
                <DrawerFooter>
                    <IconButton onClick={toggleDrawer}>
                        {drawerOpen ? <ChevronLeftIcon /> : <ChevronRightIcon />}
                    </IconButton>
                </DrawerFooter>
            </Drawer>
            <Box component="main" sx={{ flexGrow: 1, p: 3 }}>
                <Toolbar />
                <Container maxWidth="lg">{props.children}</Container>
            </Box>
        </Box>
    )
}
