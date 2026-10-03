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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergMaterializedViewDefinition
{
    @Test
    public void testRoundTrip()
    {
        List<IcebergMaterializedViewDefinition.Column> columns = ImmutableList.of(
                new IcebergMaterializedViewDefinition.Column("a", TypeId.of("integer"), Optional.empty()));
        IcebergMaterializedViewDefinition definition = new IcebergMaterializedViewDefinition(
                "SELECT a FROM base",
                Optional.of("iceberg"),
                Optional.of("schema"),
                columns,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());

        assertThat(decodeMaterializedViewData(encodeMaterializedViewData(definition))).isEqualTo(definition);
    }

    @Test
    public void testDecodeNullData()
    {
        // Regression for EGANP-6330: a materialized view whose ViewOriginalText is null in the
        // metastore must surface a clean TrinoException, not an unguarded NullPointerException
        // (which escaped to the user as GENERIC_INTERNAL_ERROR).
        assertThatThrownBy(() -> decodeMaterializedViewData(null))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Materialized View data is null");
    }

    @Test
    public void testDecodeMissingPrefix()
    {
        assertThatThrownBy(() -> decodeMaterializedViewData("not a materialized view"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("missing prefix");
    }
}
