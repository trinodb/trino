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

// Height of the fixed app bar; sticky elements below it offset by this much
export const APP_BAR_HEIGHT = 43

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
        NotImplementedAuthType: 'The configured authentication type is not supported',
    },
    Api: {
        FetchingData: 'Fetching data...',
        Stats: {
            Name: 'Cluster statistics',
        },
    },
    Error: {
        NotImplemented: 'This feature is not implemented',
        Communication: 'Communication error to Trino.',
        Forbidden: 'Forbidden',
        Network: 'The network has wandered off, please try again later!',
        NodeInformationNotLoaded: 'Node information could not be loaded',
        QueryInformationNotLoaded: 'Query information could not be loaded',
        QueryListNotLoaded: 'Query list could not be loaded',
        QueryNotFound: 'Query not found',
    },
    Menu: {
        Header: {
            Home: 'Dashboard',
            LightMode: 'Switch to light mode',
            DarkMode: 'Switch to dark mode',
            HideQueryText: 'Hide query text',
            ShowQueryText: 'Show query text',
            Logout: 'Logout',
        },
    },
    QueryList: {
        NoQueries: 'No Queries',
        NoMatchedFilterQueries: 'No queries matched filters',
        Actions: {
            Copy: 'Copy query text',
            Copied: 'Query text copied',
            CopyFailed: 'Could not copy to the clipboard',
            Kill: 'Kill query',
            KillRequested: 'Kill requested',
            KillConfirmTitle: 'Kill this query?',
            KillConfirmCancel: 'Cancel',
            AlreadyDone: 'This query has already finished',
            Expand: 'Show full query',
            Collapse: 'Show less',
        },
        Filter: {
            Search: 'Search',
            SearchFilters: 'Search filters',
            MyQueries: 'My queries',
            AllQueries: 'All',
            SearchPlaceholder:
                'User, source, query ID, state, resource group, error name, query text, client tags or trace token',
            State: 'State',
            Type: {
                RUNNING: 'Running',
                QUEUED: 'Queued',
                FINISHED: 'Finished',
            },

            ReorderInterval: 'Reorder Interval',
            Limit: 'Limit',
        },
    },
}
