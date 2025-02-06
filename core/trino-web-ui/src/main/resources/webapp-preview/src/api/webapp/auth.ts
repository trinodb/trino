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
import { api, ApiResponse } from '../base'

export interface AuthInfo {
    authType: 'insecure' | 'form' | 'fixed' | 'oauth2' | null
    passwordAllowed: boolean
    authenticated: boolean
    username?: string
}

export type Empty = object

export async function authInfoApi(): Promise<ApiResponse<AuthInfo>> {
    return api.get('/ui/preview/auth/info')
}

export async function loginApi(body: Record<string, string>): Promise<ApiResponse<Empty>> {
    return api.post('/ui/preview/auth/login', body)
}

export async function logoutApi(): Promise<ApiResponse<Empty>> {
    return api.get('/ui/preview/auth/logout')
}
