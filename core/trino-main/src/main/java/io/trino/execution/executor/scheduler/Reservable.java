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
package io.trino.execution.executor.scheduler;

/**
 * An entity that can hold at most one {@link Reservation} slot at a time, and that tracks
 * that fact itself so the reservation does not need a shared registry to detect
 * double-reserving or double-releasing.
 */
interface Reservable
{
    /**
     * @return false if this entity already holds a slot
     */
    boolean tryMarkReserved();

    /**
     * @return false if this entity does not currently hold a slot
     */
    boolean tryMarkReleased();
}
