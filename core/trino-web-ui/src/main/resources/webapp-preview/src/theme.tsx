import { createTheme } from '@mui/material/styles'

export const lightTheme = createTheme({
    palette: {
        mode: 'light',
        primary: {
            main: '#0b1367',
        },
        secondary: {
            main: '#f50057',
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
    },
})

export const darkTheme = createTheme({
    palette: {
        mode: 'dark',
    },
    components: {
        MuiLink: {
            styleOverrides: {
                root: {
                    color: '#dd33fa',
                    textDecoration: 'none',
                },
            },
        },
    },
})
