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
package io.trino.plugin.paimon;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;

import static io.trino.plugin.paimon.PaimonTypeUtils.toTrinoType;
import static java.util.Objects.requireNonNull;

public record PaimonColumnHandle(String columnName, String typeString, Type trinoType, int columnId)
        implements ColumnHandle
{
    public static final String TRINO_ROW_ID_NAME = "$row_id";

    public PaimonColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(typeString, "typeString is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    public static PaimonColumnHandle of(String columnName, DataType columnType, int columnId)
    {
        return new PaimonColumnHandle(
                columnName,
                JsonSerdeUtil.toJson(columnType),
                toTrinoType(columnType),
                columnId);
    }

    @JsonProperty
    public boolean isRowId()
    {
        return TRINO_ROW_ID_NAME.equals(columnName);
    }
}
