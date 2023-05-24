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
package io.trino.plugin.kudu;

import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseKuduWithDisabledInferSchemaConnectorSmokeTest
        extends BaseKuduConnectorSmokeTest
{
    @Override
    protected Optional<String> getKuduSchemaEmulationPrefix()
    {
        return Optional.empty();
    }

    @Test
    public void testListingOfTableForDefaultSchema()
    {
        // Test methods may run in parallel and create tables in the default schema
        // Assert at least the TPCH tables exist but there may be more
        List<String> rows = new ArrayList<>(computeActual("SHOW TABLES FROM default").getMaterializedRows())
                .stream()
                .map(row -> ((String) row.getField(0)))
                .collect(toUnmodifiableList());
        assertThat(rows).containsAll(
                REQUIRED_TPCH_TABLES.stream()
                        .map(TpchTable::getTableName)
                        .collect(Collectors.toList()));
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }
}
