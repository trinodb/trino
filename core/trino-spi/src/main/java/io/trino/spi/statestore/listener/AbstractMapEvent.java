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
package io.trino.spi.statestore.listener;

import io.trino.spi.statestore.Member;

public abstract class AbstractMapEvent
{
    protected final EntryEventType eventType;
    protected final Member member;

    public AbstractMapEvent(Member member, int eventType)
    {
        this.eventType = EntryEventType.fromTypeId(eventType);
        this.member = member;
    }

    public EntryEventType getEventType()
    {
        return eventType;
    }

    public Member getMember()
    {
        return member;
    }

    @Override
    public String toString()
    {
        return "AbstractMapEvent{" +
                "eventType=" + eventType +
                ", member=" + member +
                '}';
    }
}
