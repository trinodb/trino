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

// Polls the coordinator gently: one request at a time (the next starts only after the previous answered), starts
// at least intervalMs apart, and nothing while the browser tab is hidden. Returns a function that stops the loop.
export const pollCoordinator = (request: () => Promise<unknown>, intervalMs: number): (() => void) => {
    let active = true
    let timeoutId: number
    const run = async () => {
        const started = Date.now()
        if (!document.hidden) {
            await request()
        }
        if (active) {
            timeoutId = window.setTimeout(run, Math.max(0, intervalMs - (Date.now() - started)))
        }
    }
    run()
    return () => {
        active = false
        window.clearTimeout(timeoutId)
    }
}
