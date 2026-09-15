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
import { afterEach, beforeEach, expect, jest, test } from 'bun:test'
import { act, createElement } from 'react'
import { createRoot } from 'react-dom/client'
import { createMemoryRouter, RouterProvider } from 'react-router-dom'
import { api } from '../api/base.ts'
import { Dashboard } from '../components/Dashboard.tsx'
import { WorkersList } from '../components/WorkersList.tsx'
import { WorkerStatus } from '../components/WorkerStatus.tsx'
import { SnackbarContext } from '../components/SnackbarContext.ts'

let root
let container
let router
let requests
let originalAdapter
let notifications

beforeEach(() => {
    jest.useFakeTimers()
    requests = []
    notifications = []
    originalAdapter = api.axiosInstance.defaults.adapter
    api.axiosInstance.defaults.adapter = async (config) => {
        requests.push(config.url)
        let data = []
        if (config.url === '/ui/api/stats') {
            data = {
                runningQueries: 0,
                queuedQueries: 0,
                blockedQueries: 0,
                activeWorkers: 1,
                runningDrivers: 0,
                reservedMemory: 0,
                totalInputRows: 0,
                totalInputBytes: 0,
                totalCpuTimeSecs: 0,
            }
        }
        // A missing worker still exercises the polling lifecycle without chart layout.
        return { data, status: config.url.endsWith('/status') ? 404 : 200, statusText: 'OK', headers: {}, config }
    }
    localStorage.clear()
    container = document.createElement('div')
    document.body.appendChild(container)
    root = createRoot(container)
    router = createMemoryRouter([
        { path: '/', element: createElement('div', null, 'Home') },
        { path: '/dashboard', element: createElement(Dashboard) },
        { path: '/workers', element: createElement(WorkersList) },
        { path: '/workers/:nodeId', element: createElement(WorkerStatus) },
    ])
})

afterEach(async () => {
    await act(async () => root.unmount())
    router.dispose()
    container.remove()
    api.axiosInstance.defaults.adapter = originalAdapter
    localStorage.clear()
    jest.useRealTimers()
})

async function render() {
    await act(async () =>
        root.render(
            createElement(
                SnackbarContext.Provider,
                { value: { showSnackbar: (message) => notifications.push(message) } },
                createElement(RouterProvider, { router })
            )
        )
    )
}

async function navigate(path) {
    await act(async () => router.navigate(path))
}

async function tick() {
    await act(async () => jest.advanceTimersByTime(1000))
}

for (const [path, endpoint] of [
    ['/dashboard', '/ui/api/stats'],
    ['/workers', '/ui/api/worker'],
    ['/workers/worker_a', '/ui/api/worker/worker_a/status'],
]) {
    test(`${path} stops polling on navigation and starts one loop on return`, async () => {
        await render()
        await navigate(path)
        await tick()
        await navigate('/')
        const count = requests.filter((url) => url === endpoint).length
        await tick()
        await tick()
        expect(requests.filter((url) => url === endpoint)).toHaveLength(count)
        await navigate(path)
        await tick()
        expect(requests.filter((url) => url === endpoint)).toHaveLength(count + 2)
    })
}

test('changing worker ID stops polling the previous worker', async () => {
    await render()
    await navigate('/workers/worker_a')
    await tick()
    const count = requests.filter((url) => url.includes('worker_a')).length
    await navigate('/workers/worker_b')
    await tick()
    expect(requests.filter((url) => url.includes('worker_a'))).toHaveLength(count)
    expect(requests.filter((url) => url.includes('worker_b'))).toHaveLength(2)
})

test('a response arriving after navigation does not restart polling', async () => {
    let respond
    api.axiosInstance.defaults.adapter = (config) => {
        requests.push(config.url)
        return new Promise((resolve) => {
            respond = () => resolve({ data: [], status: 200, statusText: 'OK', headers: {}, config })
        })
    }
    await render()
    await navigate('/workers')
    await navigate('/')
    await act(async () => respond())
    await tick()
    expect(requests).toHaveLength(1)
})

test('a late response from the previous worker cannot replace the current error', async () => {
    let respond
    api.axiosInstance.defaults.adapter = (config) => {
        requests.push(config.url)
        if (config.url.includes('worker_a')) {
            return new Promise((resolve) => {
                respond = () => resolve({ data: {}, status: 503, statusText: 'obsolete worker', headers: {}, config })
            })
        }
        return Promise.resolve({ data: {}, status: 503, statusText: 'current worker', headers: {}, config })
    }
    await render()
    await navigate('/workers/worker_a')
    await navigate('/workers/worker_b')
    expect(notifications).toHaveLength(1)
    expect(notifications[0]).toContain('current worker')
    await act(async () => respond())
    expect(notifications).toHaveLength(1)
    expect(notifications[0]).toContain('current worker')
})
