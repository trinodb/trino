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
package io.trino.plugin.lakehouse;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.lakehouse.LakehouseConnector.mergeTableProcedures;
import static io.trino.plugin.lakehouse.TableType.DELTA;
import static io.trino.plugin.lakehouse.TableType.HIVE;
import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestLakehouseConnector
{
    @Test
    void testProcedureDeclaredByMultipleTableTypesIsExposedOnce()
    {
        TableProcedureMetadata hive = tableProcedure(stringProperty("retention", "retention", "7d", false));
        TableProcedureMetadata iceberg = tableProcedure(stringProperty("retention", "retention", "7d", false));

        assertThat(mergeTableProcedures(ImmutableMap.of(HIVE, ImmutableSet.of(hive), ICEBERG, ImmutableSet.of(iceberg))))
                .containsExactly(hive);
    }

    @Test
    void testDifferentPropertyDefaultIsRejected()
    {
        TableProcedureMetadata hive = tableProcedure(stringProperty("retention", "retention", "7d", false));
        TableProcedureMetadata delta = tableProcedure(stringProperty("retention", "retention", "30d", false));

        assertThatThrownBy(() -> mergeTableProcedures(ImmutableMap.of(HIVE, ImmutableSet.of(hive), DELTA, ImmutableSet.of(delta))))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("Table procedure TEST declares different properties");
    }

    @Test
    void testDifferentPropertyTypeIsRejected()
    {
        TableProcedureMetadata hive = tableProcedure(stringProperty("retention", "retention", null, false));
        TableProcedureMetadata delta = tableProcedure(integerProperty("retention", "retention", null, false));

        assertThatThrownBy(() -> mergeTableProcedures(ImmutableMap.of(HIVE, ImmutableSet.of(hive), DELTA, ImmutableSet.of(delta))))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("Table procedure TEST declares different properties");
    }

    @Test
    void testDifferentPropertyNameIsRejected()
    {
        TableProcedureMetadata hive = tableProcedure(stringProperty("retention", "retention", "7d", false));
        TableProcedureMetadata delta = tableProcedure(stringProperty("retention_threshold", "retention", "7d", false));

        assertThatThrownBy(() -> mergeTableProcedures(ImmutableMap.of(HIVE, ImmutableSet.of(hive), DELTA, ImmutableSet.of(delta))))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("Table procedure TEST declares different properties");
    }

    @Test
    void testDifferentExecutionModeIsRejected()
    {
        TableProcedureMetadata hive = new TableProcedureMetadata("TEST", distributedWithFilteringAndRepartitioning(), ImmutableList.of());
        TableProcedureMetadata delta = new TableProcedureMetadata("TEST", coordinatorOnly(), ImmutableList.of());

        assertThatThrownBy(() -> mergeTableProcedures(ImmutableMap.of(HIVE, ImmutableSet.of(hive), DELTA, ImmutableSet.of(delta))))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("Table procedure TEST declares different reads data execution mode");
    }

    private static TableProcedureMetadata tableProcedure(PropertyMetadata<?> property)
    {
        return new TableProcedureMetadata("TEST", distributedWithFilteringAndRepartitioning(), ImmutableList.of(property));
    }
}
