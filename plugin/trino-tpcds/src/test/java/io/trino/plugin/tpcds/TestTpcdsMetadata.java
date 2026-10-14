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
package io.trino.plugin.tpcds;

import io.trino.spi.connector.ConnectorSession;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.tpcds.TpcdsMetadata.identifierType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTpcdsMetadata
{
    private final TpcdsMetadata tpcdsMetadata = new TpcdsMetadata();
    private final ConnectorSession session = null;

    @Test
    public void testHiddenSchemas()
    {
        assertThat(tpcdsMetadata.schemaExists(session, "sf1")).isTrue();
        assertThat(tpcdsMetadata.schemaExists(session, "sf3000.0")).isTrue();
        assertThat(tpcdsMetadata.schemaExists(session, "sf0")).isFalse();
        assertThat(tpcdsMetadata.schemaExists(session, "hf1")).isFalse();
        assertThat(tpcdsMetadata.schemaExists(session, "sf")).isFalse();
        assertThat(tpcdsMetadata.schemaExists(session, "sfabc")).isFalse();
    }

    @Test
    public void testIdentifierType()
    {
        assertThat(identifierType(0.01)).isEqualTo(INTEGER);
        assertThat(identifierType(1)).isEqualTo(INTEGER);
        assertThat(identifierType(3000)).isEqualTo(INTEGER);
        assertThat(identifierType(10000)).isEqualTo(BIGINT);
        assertThat(identifierType(100000)).isEqualTo(BIGINT);
    }
}
