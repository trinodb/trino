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
package io.prestosql.plugin.google.sheets;

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

public final class SheetsHelper
{
    @Inject
    private SheetsHelper()
    {
    }

    public static Type getType(String type)
    {
        switch (type) {
            case "bigint":
                return BigintType.BIGINT;
            case "integer":
                return IntegerType.INTEGER;
            case "smallint":
                return SmallintType.SMALLINT;
            case "tinyint":
                return TinyintType.TINYINT;
            case "real":
                return RealType.REAL;
            case "double":
                return DoubleType.DOUBLE;
            case "varbinary":
                return VarbinaryType.VARBINARY;
            case "time":
                return TimeType.TIME;
            case "timestamp":
                return TimestampType.TIMESTAMP;
            case "date":
                return DateType.DATE;
            default:
                return VarcharType.VARCHAR;
        }
    }
}
