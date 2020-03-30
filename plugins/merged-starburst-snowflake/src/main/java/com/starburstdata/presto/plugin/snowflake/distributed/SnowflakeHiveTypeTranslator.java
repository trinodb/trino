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
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static io.prestosql.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

/**
 * The distributed Snowflake connector is relying on the Hive connector, but
 * Hive does not have a TIME type.  To be able to create HiveColumns for TIME,
 * we're using the TIMESTAMP type info; that works because in the Snowflake connector
 * we're getting the column metadata through JDBC, so the type info isn't really used.
 */
public class SnowflakeHiveTypeTranslator
        extends HiveTypeTranslator
{
    @Override
    public TypeInfo translate(Type type)
    {
        if (TIME.equals(type)) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        return super.translate(type);
    }
}
