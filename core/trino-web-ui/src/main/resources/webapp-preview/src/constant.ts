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
export enum StoreKey {
    Config = 'app-config',
}

export const Texts = {
    Auth: {
        Authenticating: 'Authenticating to Trino...',
        LoginForm: {
            PasswordNotAllowed: 'Password not allowed',
            Username: 'Username',
            Password: 'Password',
            UsernameMustBeDefined: 'Username must be defined',
            LogIn: 'Log in',
            LoggingIn: 'Logging in...',
        },
        Logout: 'Log Out',
        InvalidUsernameOrPassword: 'Invalid username or password',
        NotAvailableAuthInfo: 'Authentication information not available',
        NotImplementedAuthType: 'The configured authentication type is not implemented for the frontend',
    },
    Api: {
        FetchingData: 'Fetching data...',
        Stats: {
            Name: 'Cluster Statistics',
        },
    },
    Error: {
        NotImplemented: 'This feature is not implemented',
        Communication: 'Communication error to Trino.',
        Forbidden: 'Forbidden',
        Network: 'The network has wandered off, please try again later!',
        NodeInformationNotLoaded: 'Node information could not be loaded',
    },
    Menu: {
        Header: {
            Themes: 'Themes',
            Settings: 'Settings',
            Profile: 'Profile',
            Logout: 'Logout',
        },
        Drawer: {
            Dashboard: 'Dashboard',
            Workers: 'Workers',
            QueryHistory: 'Query History',
            DemoComponents: 'Demo Components',
        },
    },
}
