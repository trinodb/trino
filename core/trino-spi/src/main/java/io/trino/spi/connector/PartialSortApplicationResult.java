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
package io.trino.spi.connector;

public class PartialSortApplicationResult<T>
{
    private final T handle;
    private final boolean sameSortDirection;
    private final boolean sameNullOrdering;
    private final boolean allSorted;

    public PartialSortApplicationResult(T handle, boolean sortDirection, boolean nullOrdering, boolean allSorted)
    {
        this.handle = handle;
        this.sameSortDirection = sortDirection;
        this.sameNullOrdering = nullOrdering;
        this.allSorted = allSorted;
    }

    public T getHandle()
    {
        return handle;
    }

    public boolean isSameSortDirection()
    {
        return sameSortDirection;
    }

    public boolean isSameNullOrdering()
    {
        return sameNullOrdering;
    }

    public boolean allSorted()
    {
        return allSorted;
    }
}
