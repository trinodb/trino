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
import axios, { AxiosError, AxiosInstance, AxiosResponse } from 'axios'

export interface ApiResponse<T> {
    status: number | undefined
    message: string
    data?: T
}

export class ClientApi {
    axiosInstance: AxiosInstance = axios.create()

    paramsToQueryString = (params: Record<string, string>): string => {
        let queryString = ''
        if (Object.keys(params).length > 0) {
            queryString = '?' + new URLSearchParams(params).toString()
        }
        return queryString
    }

    fetchData = async <T, P = undefined>(url: string, method: 'GET' | 'POST', params?: P): Promise<ApiResponse<T>> => {
        try {
            let response: AxiosResponse<T>
            if (method === 'GET') {
                response = await this.axiosInstance.get(url)
            } else if (method === 'POST') {
                response = await this.axiosInstance.post(url, params)
            } else {
                throw new Error(`Unsupported HTTP method: ${method}`)
            }
            return {
                status: response.status,
                message: response.statusText,
                data: response.data,
            }
        } catch (error) {
            if (axios.isAxiosError(error)) {
                const axiosError = error as AxiosError
                return {
                    status: axiosError.status,
                    message: axiosError.message,
                }
            }
            throw error
        }
    }

    get = async <T>(url: string, params: Record<string, string> = {}): Promise<ApiResponse<T>> => {
        return this.fetchData(url + this.paramsToQueryString(params), 'GET')
    }

    post = async <T>(url: string, body: Record<string, string> = {}): Promise<ApiResponse<T>> => {
        return this.fetchData(url, 'POST', body)
    }
}

export const api = new ClientApi()
