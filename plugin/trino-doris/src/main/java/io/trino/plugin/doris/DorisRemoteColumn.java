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
package io.trino.plugin.doris;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DorisRemoteColumn(
        String columnName,
        String dataType,
        Optional<Integer> columnSize,
        Optional<Integer> decimalDigits,
        int ordinalPosition,
        Optional<String> typeDefinition)
{
    public DorisRemoteColumn(String columnName, String dataType, Optional<Integer> columnSize, Optional<Integer> decimalDigits, int ordinalPosition)
    {
        this(columnName, dataType, columnSize, decimalDigits, ordinalPosition, Optional.empty());
    }

    public DorisRemoteColumn(
            String columnName,
            String dataType,
            Optional<Integer> columnSize,
            Optional<Integer> decimalDigits,
            int ordinalPosition,
            Optional<String> typeDefinition)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.columnSize = requireNonNull(columnSize, "columnSize is null");
        this.decimalDigits = requireNonNull(decimalDigits, "decimalDigits is null");
        this.ordinalPosition = ordinalPosition;
        this.typeDefinition = requireNonNull(typeDefinition, "typeDefinition is null");
    }
}
