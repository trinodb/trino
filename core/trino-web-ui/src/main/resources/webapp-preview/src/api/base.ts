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
export interface ApiResponse {
  ok: boolean;
  status: number;
  statusText: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  result?: any;
}

export class ClientApi
{
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async fetchApi(url: string, method: string = 'GET', body: Record<string, any> | undefined = undefined)
  {
    const response = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    }).catch((err) => {
      return new Response(JSON.stringify({ error: err }), {
        status: 500,
        headers: {
          "Content-Type": "application/json",
        },
      });
    });

    // Return early http errors
    if (response.status !== 200) {
      return { ok: false, status: response.status, statusText: response.statusText }
    }

    // Catch all known logical errors and return usable errors
    try {
      const responseContentType = response.headers.get('content-type');
      const isResponseJson = responseContentType && responseContentType.indexOf('application/json') !== -1;
      if (!isResponseJson) {
        return { ok: false, status: 500, statusText: 'Invalid content type received from server' }
      }
      const responseJson = await response.json();
      if (!responseJson) {
        return { ok: false, status: 500, statusText: 'Empty response received from server' }
      }
      return { ok: response.ok, status: response.status, statusText: response.statusText, result: responseJson };
    } catch (error) {
      if (error instanceof SyntaxError) {
        return { ok: false, status: 500, statusText: 'Invalid JSON received from server' }
      } else if (error instanceof Error) {
        return { ok: false, status: 500, statusText: 'Invalid response received from server.' + error }
      }
    }
    return { ok: false, status: 500, statusText: 'Unknown error' }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async get(url: string, params: Record<string, any> = {}): Promise<ApiResponse>
  {
    let queryString = "";
    if (Object.keys(params).length > 0) {
      queryString = "?" + new URLSearchParams(params).toString();
    }
    return await this.fetchApi(url + queryString, 'GET');
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async post(url: string, body: Record<string, any> = {}): Promise<ApiResponse>
  {
    return await this.fetchApi(url, 'POST', body);
  }
}

export const api = new ClientApi();
