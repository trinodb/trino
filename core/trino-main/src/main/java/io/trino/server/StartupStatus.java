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

public final class StartupStatus
{
    private final AtomicBoolean startupComplete = new AtomicBoolean();

    public void startupComplete()
    {
        checkState(startupComplete.compareAndSet(false, true), "Server startup already marked as complete");
    }

    public boolean isStartupComplete()
    {
        return startupComplete.get();
    }
}
