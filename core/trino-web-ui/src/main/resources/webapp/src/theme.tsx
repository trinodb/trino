import { createTheme } from '@mui/material/styles'

export const lightTheme = createTheme({
    palette: {
        mode: 'light',
        background: {
            default: '#fff',
            paper: '#f6f9f9',
        },
        primary: {
            main: '#000033',
        },
        secondary: {
            main: '#001c93',
        },
        info: {
            main: '#001c93',
        },
    },
    components: {
        MuiLink: {
            styleOverrides: {
                root: {
                    color: '#f50057',
                    textDecoration: 'none',
                },
            },
        },
        MuiAppBar: {
            styleOverrides: {
                root: {
                    backgroundColor: '#000033',
                },
            },
        },
    },
})

export const darkTheme = createTheme({
    palette: {
        mode: 'dark',
        background: {
            default: '#272727',
            paper: '#212121',
        },
        primary: {
            main: '#fff',
        },
        secondary: {
            main: '#006ef8',
        },
        info: {
            main: '#9e9e9e',
        },
    },
    components: {
        MuiLink: {
            styleOverrides: {
                root: {
                    color: '#006ef8',
                    textDecoration: 'none',
                },
            },
        },
    },
})
