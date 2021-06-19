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
package io.trino.operator;

/**
 * Reference to a row.
 * <p>
 * Note: RowReference gives us the ability to defer row ID generation (which can be expensive in tight loops).
 */
public interface RowReference
{
    /**
     * Compares the referenced row to the specified row ID using the provided RowIdComparisonStrategy.
     */
    int compareTo(RowIdComparisonStrategy strategy, long rowId);

    /**
     * Checks equality of the referenced row with the specified row ID using the provided RowIdHashStrategy.
     */
    boolean equals(RowIdHashStrategy strategy, long rowId);

    /**
     * Calculates the hash of the referenced row using the provided RowIdHashStrategy.
     */
    long hash(RowIdHashStrategy strategy);

    /**
     * Allocate a stable row ID that can be used to reference this row at a future point.
     */
    long allocateRowId();
}
