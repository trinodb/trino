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
package io.trino.plugin.pinot;

import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPinotSessionProperties
{
    @Test
    public void testInvalidNumSegmentSplits()
    {
        assertThatThrownBy(() -> {
            new PinotConfig().setSegmentsPerSplit(-3);
        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConnectionTimeoutParsedProperly()
    {
        PinotConfig pinotConfig = new PinotConfig().setConnectionTimeout(new Duration(15, TimeUnit.SECONDS));
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(pinotConfig);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(pinotSessionProperties.getSessionProperties())
                .build();
        assertThat(PinotSessionProperties.getConnectionTimeout(session)).isEqualTo(new Duration(0.25, TimeUnit.MINUTES));
    }
}
