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
package io.trino.plugin.druid;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Optional;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDruidClient
{
    private static final DruidJdbcClient JDBC_CLIENT = new DruidJdbcClient(
            new BaseJdbcConfig(),
            session -> {
                throw new UnsupportedOperationException();
            },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    @Test
    public void testMapsDecimalWithNegativeScale()
    {
        JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                Types.DECIMAL,
                Optional.of("DECIMAL"),
                Optional.of(5),
                Optional.of(-3),
                Optional.empty(),
                Optional.empty());

        Optional<ColumnMapping> columnMapping = JDBC_CLIENT.toColumnMapping(SESSION, null, typeHandle);

        assertThat(columnMapping).isPresent();
        assertThat(columnMapping.orElseThrow().getType()).isEqualTo(createDecimalType(8, 0));
    }
}
