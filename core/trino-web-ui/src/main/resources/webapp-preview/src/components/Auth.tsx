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
import { Box, CircularProgress, Alert, Typography } from '@mui/material'
import { useAuth } from './AuthContext'
import { Texts } from '../constant'
import { Login } from './Login'

export const Auth = () => {
    const { authInfo, error, loading } = useAuth()

    if (!authInfo) {
        return (
            <Box sx={{ p: 10 }} display="flex" flexDirection="column" alignItems="center" justifyContent="center">
                {loading ? (
                    <>
                        <CircularProgress />
                        <Typography mt={2}>{Texts.Auth.Authenticating}</Typography>
                    </>
                ) : (
                    error && <Alert severity="error">{error}</Alert>
                )}
            </Box>
        )
    }

    if (authInfo.authType === 'form' && !authInfo?.authenticated) {
        return <Login passwordAllowed={authInfo.passwordAllowed} />
    }

    return <div>{Texts.Auth.NotImplementedAuthType}</div>
}
