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

/**
 *
 * Event that is triggered on any changes to StateStore Map collection.
 *
 */
public class EntryEvent<K, V>
        extends AbstractMapEvent
{
    /**
     * Key of the StateMap on which an operation
     */
    private K key;

    /**
     * Old value associated with the Key
     */
    private V oldValue;

    /**
     * New value for the key
     */
    private V value;

    public EntryEvent(Member member, int eventType, K key, V value)
    {
        super(member, eventType);
        this.key = key;
        this.value = value;
        this.oldValue = null;
    }

    public EntryEvent(Member member, int eventType, K key, V oldValue, V value)
    {
        super(member, eventType);
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
    }

    public K getKey()
    {
        return key;
    }

    public V getOldValue()
    {
        return oldValue;
    }

    public V getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return "EntryEvent{" +
                "key=" + key +
                ", oldValue=" + oldValue +
                ", value=" + value +
                '}';
    }
}
