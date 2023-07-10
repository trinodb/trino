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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.plugin.hudi.HudiSessionProperties.getColumnsToHide;
import static org.testng.Assert.assertEqualsNoOrder;

public class TestHudiSessionProperties
{
    @Test
    public void testSessionPropertyColumnsToHide()
    {
        HudiConfig config = new HudiConfig()
                .setColumnsToHide("col1, col2");
        HudiSessionProperties sessionProperties = new HudiSessionProperties(config, new ParquetReaderConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
        List<String> expectedColumnsToHide = ImmutableList.of("col1", "col2");
        List<String> actualColumnsToHide = getColumnsToHide(session);
        assertEqualsNoOrder(expectedColumnsToHide.toArray(), actualColumnsToHide.toArray());
    }
}
