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
package io.trino.plugin.hudi;

import org.apache.avro.Schema.Type;

import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.CHAR;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.DECIMAL;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.SMALLINT;
import static io.trino.spi.type.StandardTypes.TIME;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.StandardTypes.UUID;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;

public final class TypeConverter
{
    private TypeConverter() {}

    public static String toSparkType(io.trino.spi.type.Type type)
    {
        switch (type.getBaseName()) {
            case BOOLEAN:
                return "boolean";
            case UUID:
            case VARCHAR:
            case CHAR:
                return "string";
            case TIMESTAMP:
                return "timestamp";
            case TIME:
            case DATE:
                return "datetime";
            case DOUBLE:
            case DECIMAL:
                return "double";
            case BIGINT:
                return "long";
            case TINYINT:
            case SMALLINT:
                return "short";
            case INTEGER:
                return "integer";
            case ARRAY:
                return "array";
        }
        throw new UnsupportedOperationException(format("Cannot convert from StandardTypes type '%s' (%s) to spark type", type, type.getBaseName()));
    }

    public static Type toSchemaType(io.trino.spi.type.Type type)
    {
        switch (type.getBaseName()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case CHAR:
                return Type.STRING;
            case DATE:
                return Type.INT;
            case DOUBLE:
            case DECIMAL:
                return Type.DOUBLE;
            case BIGINT:
                return Type.LONG;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case TIMESTAMP:
            case TIME:
                return Type.INT;
            case VARCHAR:
                return Type.STRING;
            case UUID:
                return Type.STRING;
            case ARRAY:
                return Type.ARRAY;
        }
        throw new UnsupportedOperationException(format("Cannot convert from StandardTypes type '%s' (%s) to avro.Schema type", type, type.getBaseName()));
    }
}
