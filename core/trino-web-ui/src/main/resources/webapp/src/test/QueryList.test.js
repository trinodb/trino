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
import { MemoryRouter } from 'react-router-dom'
import { api } from '../api/base.ts'
import { QueryList } from '../components/QueryList.tsx'

let root
let container
let queries
let requests
let originalAdapter

function query(id, cpu) {
    return {
        queryId: id,
        state: 'RUNNING',
        scheduled: true,
        sessionUser: 'test',
        sessionSource: 'test',
        resourceGroupId: ['global'],
        queryTextPreview: 'SELECT 1',
        clientTags: [],
        queryStats: {
            createTime: '2026-01-01T00:00:00Z',
            totalCpuTime: `${cpu}s`,
            elapsedTime: '1s',
            executionTime: '1s',
            totalMemoryReservation: '1MB',
            peakTotalMemoryReservation: '1MB',
            cumulativeUserMemory: 0,
            queuedDrivers: 0,
            runningDrivers: 1,
            completedDrivers: 0,
            fullyBlocked: false,
            progressPercentage: 10,
        },
    }
}

beforeEach(() => {
    jest.useFakeTimers()
    localStorage.clear()
    localStorage.setItem('sortType', JSON.stringify('CPU'))
    requests = 0
    queries = [query('query_a', 10), query('query_b', 5)]
    originalAdapter = api.axiosInstance.defaults.adapter
    api.axiosInstance.defaults.adapter = async (config) => {
        requests++
        return { data: queries, status: 200, statusText: 'OK', headers: {}, config }
    }
    container = document.createElement('div')
    document.body.appendChild(container)
    root = createRoot(container)
})

afterEach(async () => {
    await act(async () => root.unmount())
    container.remove()
    api.axiosInstance.defaults.adapter = originalAdapter
    localStorage.clear()
    jest.useRealTimers()
})

async function render(interval) {
    localStorage.setItem('reorderInterval', JSON.stringify(interval))
    await act(async () => root.render(createElement(MemoryRouter, null, createElement(QueryList))))
}

async function tick(milliseconds) {
    await act(async () => jest.advanceTimersByTime(milliseconds))
}

function displayedIds() {
    return [...container.querySelectorAll('a[href^="/queries/"]')].map((link) => link.textContent)
}

test('Off refreshes metrics without moving existing rows and appends new queries', async () => {
    await render(0)
    expect(displayedIds()).toEqual(['query_a', 'query_b'])
    queries = [query('query_a', 11), query('query_b', 20), query('query_c', 30)]
    await tick(1000)
    expect(container.textContent).toContain('20s')
    expect(displayedIds()).toEqual(['query_a', 'query_b', 'query_c'])
    queries = [query('query_b', 21), query('query_c', 31)]
    await tick(1000)
    expect(displayedIds()).toEqual(['query_b', 'query_c'])
})

test('reordering waits for each interval, including after the first reorder', async () => {
    await render(5000)
    queries = [query('query_a', 11), query('query_b', 20)]
    await tick(1000)
    expect(displayedIds()).toEqual(['query_a', 'query_b'])
    await tick(5000)
    expect(displayedIds()).toEqual(['query_b', 'query_a'])
    queries = [query('query_a', 30), query('query_b', 21)]
    await tick(1000)
    expect(displayedIds()).toEqual(['query_b', 'query_a'])
    await tick(5000)
    expect(displayedIds()).toEqual(['query_a', 'query_b'])
})

test('unmount stops polling', async () => {
    await render(0)
    await act(async () => root.unmount())
    root = createRoot(container)
    const count = requests
    await tick(5000)
    expect(requests).toBe(count)
})
