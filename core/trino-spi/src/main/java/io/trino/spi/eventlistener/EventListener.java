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
package io.trino.spi.eventlistener;

public interface EventListener
{
    default void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    default void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
    }

    default void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }

    /**
     * Specify whether the plan included in QueryCompletedEvent should be anonymized or not
     */
    default boolean requiresAnonymizedPlan()
    {
        return false;
    }

    /**
     * Shutdown the event listener by releasing any held resources such as
     * threads, sockets, etc. After this method is called,
     * no methods will be called on the event listener.
     */
    default void shutdown()
    {
    }
}
