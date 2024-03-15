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
package io.trino.plugin.jdbc;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class RemoteLogTracingEvent
        implements Consumer<RemoteDatabaseEvent>
{
    private final Predicate<RemoteDatabaseEvent> condition;
    private final AtomicBoolean happened;

    public RemoteLogTracingEvent(Predicate<RemoteDatabaseEvent> condition)
    {
        this.condition = requireNonNull(condition, "condition is null");
        this.happened = new AtomicBoolean();
    }

    public boolean hasHappened()
    {
        return happened.get();
    }

    @Override
    public void accept(RemoteDatabaseEvent remoteDatabaseEvent)
    {
        if (hasHappened()) {
            return;
        }

        if (condition.test(remoteDatabaseEvent)) {
            happened.compareAndSet(false, true);
        }
    }
}
