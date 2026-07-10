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

/**
 *
 * Type of entry event.
 *
 */
public enum EntryEventType
{
    /**
    * Fired if an entry is added.
    */
    ADDED(1),

    /**
     * Fired if an entry is removed.
     */
    REMOVED(2),

    /**
     * Fired if an entry is updated.
     */
    UPDATED(3);

    private int eventId;

    EntryEventType(int eventId)
    {
        this.eventId = eventId;
    }

    public int getTypeId()
    {
        return this.eventId;
    }

    public static EntryEventType fromTypeId(int typeId)
    {
        switch (typeId) {
            case 1:
                return ADDED;
            case 2:
                return REMOVED;
            case 3:
                return UPDATED;
            default:
                return null;
        }
    }
}
