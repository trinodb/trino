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
import { useEffect, useState } from 'react'
import {
    Box,
    Button,
    ButtonGroup,
    Card,
    CardContent,
    CircularProgress,
    Grid2 as Grid,
    TextField,
    Typography,
} from '@mui/material'
import { useAuth } from './AuthContext'
import trinoLogo from '../assets/trino.svg'
import { Texts } from '../constant'
import { useSnackbar } from './SnackbarContext.ts'

export interface ILoginProps {
    passwordAllowed: boolean
}

interface FormElements extends HTMLFormControlsCollection {
    usernameInput: HTMLInputElement
    passwordInput: HTMLInputElement
}

interface LoginFormElement extends HTMLFormElement {
    readonly elements: FormElements
}

export const Login = (props: ILoginProps) => {
    const { login, loading, error } = useAuth()
    const { showSnackbar } = useSnackbar()
    const { passwordAllowed } = props

    const [usernameError, setUsernameError] = useState<boolean>(false)
    const [usernameErrorText, setUsernameErrorText] = useState<string>('')

    useEffect(() => {
        if (error) {
            showSnackbar(error, 'error')
        }
    }, [error, showSnackbar])

    const handleSubmit = (event: React.FormEvent<LoginFormElement>) => {
        event.preventDefault()
        const { usernameInput, passwordInput } = event.currentTarget.elements

        if (usernameInput.value.length == 0) {
            setUsernameError(true)
            setUsernameErrorText(Texts.Auth.LoginForm.UsernameMustBeDefined)
        } else {
            setUsernameError(false)
            setUsernameErrorText('')
            login(usernameInput.value, passwordInput.value)
        }
    }

    const formDisabled = loading

    return (
        <Box sx={{ p: 10, display: 'flex', justifyContent: 'center' }}>
            <Grid size={{ xs: 12 }} justifyContent="center" container>
                <form onSubmit={handleSubmit}>
                    <Card sx={{ px: 2, minWidth: 350 }}>
                        <CardContent sx={{ alignItems: 'center', textAlign: 'center' }}>
                            <Grid justifyContent="center">
                                <Box component="img" sx={{ height: 80 }} alt="logo" src={trinoLogo} />
                                <Typography component="h1" variant="h5">
                                    {Texts.Auth.LoginForm.LogIn}
                                </Typography>
                                <Box sx={{ py: 1 }}>
                                    <TextField
                                        id="usernameInput"
                                        label={Texts.Auth.LoginForm.Username}
                                        variant="outlined"
                                        error={usernameError}
                                        helperText={usernameErrorText}
                                        disabled={formDisabled}
                                        fullWidth
                                    />
                                </Box>
                                <Box sx={{ py: 1 }}>
                                    <TextField
                                        id="passwordInput"
                                        label={
                                            passwordAllowed
                                                ? Texts.Auth.LoginForm.Password
                                                : Texts.Auth.LoginForm.PasswordNotAllowed
                                        }
                                        variant={passwordAllowed ? 'outlined' : 'filled'}
                                        type="password"
                                        disabled={formDisabled || !passwordAllowed}
                                        autoComplete="on"
                                        fullWidth
                                    />
                                </Box>
                            </Grid>
                        </CardContent>
                        <Grid sx={{ py: 2, px: 2 }} justifyContent="space-between" container>
                            {loading ? (
                                <Grid container spacing={2} alignItems="center">
                                    <Grid>
                                        <CircularProgress size={28} />
                                    </Grid>
                                    <Grid>
                                        <Typography>{Texts.Auth.LoginForm.LoggingIn}</Typography>
                                    </Grid>
                                </Grid>
                            ) : (
                                <div />
                            )}
                            <ButtonGroup>
                                <Button type="submit" disabled={formDisabled}>
                                    {Texts.Auth.LoginForm.LogIn}
                                </Button>
                            </ButtonGroup>
                        </Grid>
                    </Card>
                </form>
            </Grid>
        </Box>
    )
}
