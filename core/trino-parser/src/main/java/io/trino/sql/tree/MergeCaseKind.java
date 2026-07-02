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
package io.trino.sql.tree;

/**
 * Classifies each {@code WHEN} clause in a {@code MERGE} statement by which
 * side of the join produced the row:
 *
 * <ul>
 *   <li>{@link #MATCHED} – both target and source rows are present (inner join row).</li>
 *   <li>{@link #NOT_MATCHED_BY_TARGET} – only the source row is present (right-outer null on target side).
 *       Corresponds to bare {@code WHEN NOT MATCHED} or {@code WHEN NOT MATCHED BY TARGET}.</li>
 *   <li>{@link #NOT_MATCHED_BY_SOURCE} – only the target row is present (left-outer null on source side).
 *       Corresponds to {@code WHEN NOT MATCHED BY SOURCE}.</li>
 * </ul>
 */
public enum MergeCaseKind
{
    MATCHED,
    NOT_MATCHED_BY_TARGET,
    NOT_MATCHED_BY_SOURCE,
}
