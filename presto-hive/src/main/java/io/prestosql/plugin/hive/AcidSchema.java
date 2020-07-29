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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.io.IOConstants;

import java.util.List;
import java.util.Properties;

import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_BUCKET;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_CURRENT_TRANSACTION;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_OPERATION;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_ORIGINAL_TRANSACTION;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_ROW_ID;
import static io.prestosql.plugin.hive.orc.OrcPageSourceFactory.ACID_COLUMN_ROW_STRUCT;
import static java.util.stream.Collectors.joining;

public final class AcidSchema
{
    public static final List<String> ACID_COLUMN_NAMES = ImmutableList.of(
            ACID_COLUMN_OPERATION,
            ACID_COLUMN_ORIGINAL_TRANSACTION,
            ACID_COLUMN_BUCKET,
            ACID_COLUMN_ROW_ID,
            ACID_COLUMN_CURRENT_TRANSACTION,
            ACID_COLUMN_ROW_STRUCT);

    private AcidSchema() {}

    public static Properties createAcidSchema(HiveType rowType)
    {
        Properties hiveAcidSchema = new Properties();
        hiveAcidSchema.setProperty(IOConstants.COLUMNS, String.join(",", ACID_COLUMN_NAMES));
        // We must supply an accurate row type, because Apache ORC code we don't control has a consistency
        // check that the layout of this "row" must agree with the layout of an inserted row.
        hiveAcidSchema.setProperty(IOConstants.COLUMNS_TYPES, createAcidColumnTypes(rowType).stream()
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return hiveAcidSchema;
    }

    public static List<HiveType> createAcidColumnTypes(HiveType rowType)
    {
        return ImmutableList.of(HIVE_INT, HIVE_LONG, HIVE_INT, HIVE_LONG, HIVE_LONG, rowType);
    }
}
