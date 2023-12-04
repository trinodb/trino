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
package io.trino.server;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

public class ShutdownStatus
{
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();

    public void shutdownStarted()
    {
        checkState(shutdownStarted.compareAndSet(false, true), "Server shutdown already marked as started");
    }

    public void shutdownAborted()
    {
        checkState(shutdownStarted.compareAndSet(true, false), "Server shutdown was not started");
    }

    public boolean isShutdownStarted()
    {
        return shutdownStarted.get();
    }
}
