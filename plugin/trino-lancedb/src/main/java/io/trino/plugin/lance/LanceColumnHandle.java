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
package io.trino.plugin.lance;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public record LanceColumnHandle(String name, Type trinoType, FieldType arrowType)
        implements ColumnHandle
{
    public LanceColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(trinoType, "trinoType is null");
        requireNonNull(arrowType, "arrowType is null");
    }

    public static Type toTrinoType(ArrowType type)
    {
        if (type instanceof ArrowType.Bool) {
            return BOOLEAN;
        }
        else if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                return INTEGER;
            }
            else if (intType.getBitWidth() == 64) {
                return BIGINT;
            }
        }
        else if (type instanceof ArrowType.FloatingPoint) {
            return DOUBLE;
        }
        else if (type instanceof ArrowType.Utf8) {
            return VARCHAR;
        }
        throw new UnsupportedOperationException("Unsupported arrow type: " + type);
    }

    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder().setName(name).setType(trinoType).setNullable(arrowType.isNullable()).build();
    }
}
