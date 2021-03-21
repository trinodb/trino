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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MergeCaseDetails
{
    private final int caseNumber;
    private final MergeCaseKind caseKind;
    private final Set<String> updatedColumns;

    @JsonCreator
    public MergeCaseDetails(
            @JsonProperty("caseNumber") int caseNumber,
            @JsonProperty("caseKind") MergeCaseKind caseKind,
            @JsonProperty("updatedColumns") Set<String> updatedColumns)
    {
        this.caseNumber = caseNumber;
        this.caseKind = requireNonNull(caseKind, "caseKind is null");
        this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
        switch (caseKind) {
            case DELETE:
                checkArgument(updatedColumns.isEmpty(), "For DELETE operations, updatedColumns must be empty, but is %s", updatedColumns);
                break;
            case INSERT:
            case UPDATE:
                checkArgument(!updatedColumns.isEmpty(), "For INSERT and UPDATE operations, updatedColumns must be non-empty");
                break;
        }
    }

    @JsonProperty
    public int getCaseNumber()
    {
        return caseNumber;
    }

    @JsonProperty
    public MergeCaseKind getCaseKind()
    {
        return caseKind;
    }

    @JsonProperty
    public Set<String> getUpdatedColumns()
    {
        return updatedColumns;
    }

    private static void checkArgument(boolean test, String message, Object... args)
    {
        if (!test) {
            throw new IllegalArgumentException(format(message, args));
        }
    }

    @Override
    public String toString()
    {
        return String.format("Case{caseNumber=%s, caseKind=%s, updatedColumns=%s}", caseNumber, caseKind, updatedColumns);
    }
}
