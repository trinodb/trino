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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tests.product.TestGroups.HIVE_COERCION;
import static io.trino.tests.product.TestGroups.JDBC;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestHiveCoercionOnUnpartitionedTable
        extends BaseTestHiveCoercion
{
    public static final HiveTableDefinition HIVE_COERCION_ORC = tableDefinitionBuilder("ORC")
            .setNoData()
            .build();

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat)
    {
        String tableName = format("%s_hive_coercion_unpartitioned", fileFormat.toLowerCase(ENGLISH));
        return HiveTableDefinition.builder(tableName)
                // all nested primitive coercions and adding/removing trailing nested fields are covered across row_to_row, list_to_list, and map_to_map
                .setCreateTableDDLTemplate("""
                        CREATE TABLE %NAME%(
                            row_to_row                         STRUCT<keep: STRING, ti2si: TINYINT, si2int: SMALLINT, int2bi: INT, bi2vc: BIGINT, lower2uppercase: BIGINT>,
                            list_to_list                       ARRAY<STRUCT<ti2int: TINYINT, si2bi: SMALLINT, bi2vc: BIGINT, remove: STRING>>,
                            map_to_map                         MAP<TINYINT, STRUCT<ti2bi: TINYINT, int2bi: INT, float2double: FLOAT>>,
                            tinyint_to_smallint                TINYINT,
                            tinyint_to_int                     TINYINT,
                            tinyint_to_bigint                  TINYINT,
                            smallint_to_int                    SMALLINT,
                            smallint_to_bigint                 SMALLINT,
                            int_to_bigint                      INT,
                            bigint_to_varchar                  BIGINT,
                            float_to_double                    FLOAT,
                            double_to_float                    DOUBLE,
                            shortdecimal_to_shortdecimal       DECIMAL(10,2),
                            shortdecimal_to_longdecimal        DECIMAL(10,2),
                            longdecimal_to_shortdecimal        DECIMAL(20,12),
                            longdecimal_to_longdecimal         DECIMAL(20,12),
                            float_to_decimal                   FLOAT,
                            double_to_decimal                  DOUBLE,
                            decimal_to_float                   DECIMAL(10,5),
                            decimal_to_double                  DECIMAL(10,5),
                            short_decimal_to_varchar           DECIMAL(10,5),
                            long_decimal_to_varchar            DECIMAL(20,12),
                            short_decimal_to_bounded_varchar   DECIMAL(10,5),
                            long_decimal_to_bounded_varchar    DECIMAL(20,12),
                            varchar_to_bigger_varchar          VARCHAR(3),
                            varchar_to_smaller_varchar         VARCHAR(3),
                            char_to_bigger_char                CHAR(3),
                            char_to_smaller_char               CHAR(3),
                            timestamp_to_string                TIMESTAMP,
                            timestamp_to_bounded_varchar       TIMESTAMP,
                            timestamp_to_smaller_varchar       TIMESTAMP,
                            id                                 BIGINT)
                       STORED AS\s""" + fileFormat);
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_ORC).withState(CREATED).build();
        }
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionOrc()
    {
        doTestHiveCoercion(HIVE_COERCION_ORC);
    }

    @Override
    protected Map<ColumnContext, String> expectedExceptionsWithTrinoContext()
    {
        // TODO: These expected failures should be fixed.
        return ImmutableMap.<ColumnContext, String>builder()
                // ORC
                .put(columnContext("orc", "row_to_row"), "Cannot read SQL type 'smallint' from ORC stream '.row_to_row.ti2si' of type BYTE")
                .put(columnContext("orc", "list_to_list"), "Cannot read SQL type 'integer' from ORC stream '.list_to_list.item.ti2int' of type BYTE")
                .put(columnContext("orc", "map_to_map"), "Cannot read SQL type 'integer' from ORC stream '.map_to_map.key' of type BYTE")
                .put(columnContext("orc", "tinyint_to_smallint"), "Cannot read SQL type 'smallint' from ORC stream '.tinyint_to_smallint' of type BYTE")
                .put(columnContext("orc", "tinyint_to_int"), "Cannot read SQL type 'integer' from ORC stream '.tinyint_to_int' of type BYTE")
                .put(columnContext("orc", "tinyint_to_bigint"), "Cannot read SQL type 'bigint' from ORC stream '.tinyint_to_bigint' of type BYTE")
                .put(columnContext("orc", "bigint_to_varchar"), "Cannot read SQL type 'varchar' from ORC stream '.bigint_to_varchar' of type LONG")
                .put(columnContext("orc", "double_to_float"), "Cannot read SQL type 'real' from ORC stream '.double_to_float' of type DOUBLE")
                .put(columnContext("orc", "longdecimal_to_shortdecimal"), "Decimal does not fit long (invalid table schema?)")
                .put(columnContext("orc", "float_to_decimal"), "Cannot read SQL type 'decimal(10,5)' from ORC stream '.float_to_decimal' of type FLOAT")
                .put(columnContext("orc", "double_to_decimal"), "Cannot read SQL type 'decimal(10,5)' from ORC stream '.double_to_decimal' of type DOUBLE")
                .put(columnContext("orc", "decimal_to_float"), "Cannot read SQL type 'real' from ORC stream '.decimal_to_float' of type DECIMAL")
                .put(columnContext("orc", "decimal_to_double"), "Cannot read SQL type 'double' from ORC stream '.decimal_to_double' of type DECIMAL")
                .put(columnContext("orc", "short_decimal_to_varchar"), "Cannot read SQL type 'varchar' from ORC stream '.short_decimal_to_varchar' of type DECIMAL")
                .put(columnContext("orc", "long_decimal_to_varchar"), "Cannot read SQL type 'varchar' from ORC stream '.long_decimal_to_varchar' of type DECIMAL")
                .put(columnContext("orc", "short_decimal_to_bounded_varchar"), "Cannot read SQL type 'varchar(30)' from ORC stream '.short_decimal_to_bounded_varchar' of type DECIMAL")
                .put(columnContext("orc", "long_decimal_to_bounded_varchar"), "Cannot read SQL type 'varchar(30)' from ORC stream '.long_decimal_to_bounded_varchar' of type DECIMAL")
                .buildOrThrow();
    }
}
