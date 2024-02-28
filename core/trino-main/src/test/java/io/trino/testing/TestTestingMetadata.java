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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition.Column;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTestingMetadata
{
    @Test
    public void testRenameMaterializedView()
    {
        testRenameMaterializedView("1initial", "2newName");
        testRenameMaterializedView("2initial", "1newName");
    }

    private void testRenameMaterializedView(String source, String target)
    {
        SchemaTableName initialName = schemaTableName("schema", source);
        SchemaTableName newName = schemaTableName("schema", target);
        TestingMetadata metadata = new TestingMetadata();
        ConnectorMaterializedViewDefinition viewDefinition = someMaterializedView();
        metadata.createMaterializedView(SESSION, initialName, viewDefinition, ImmutableMap.of(), false, false);

        metadata.renameMaterializedView(SESSION, initialName, newName);

        assertThat(metadata.getMaterializedView(SESSION, initialName)).isEmpty();
        assertThat(metadata.getMaterializedView(SESSION, newName)).hasValue(viewDefinition);
    }

    private static ConnectorMaterializedViewDefinition someMaterializedView()
    {
        return new ConnectorMaterializedViewDefinition(
                "select 1",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new Column("test", BIGINT.getTypeId(), Optional.empty())),
                Optional.of(Duration.ZERO),
                Optional.empty(),
                Optional.of("owner"),
                ImmutableList.of());
    }
}
