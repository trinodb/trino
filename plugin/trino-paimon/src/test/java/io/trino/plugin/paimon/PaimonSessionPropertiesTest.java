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
package io.trino.plugin.paimon;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.paimon.PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR;
import static io.trino.plugin.paimon.PaimonSessionProperties.MINIMUM_SPLIT_WEIGHT;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_CREATION_TIME;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_FILE_CREATION_TIME;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_TAG;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonSessionPropertiesTest
{
    @Test
    public void testMinimumSplitWeightDefaultsAndValidValues()
    {
        assertThat(PaimonSessionProperties.getMinimumSplitWeight(session(Map.of()))).isEqualTo(0.05);
        assertThat(PaimonSessionProperties.getMinimumSplitWeight(session(Map.of(MINIMUM_SPLIT_WEIGHT, 0.0001))))
                .isEqualTo(0.0001);
        assertThat(PaimonSessionProperties.getMinimumSplitWeight(session(Map.of(MINIMUM_SPLIT_WEIGHT, 1.0))))
                .isEqualTo(1.0);
    }

    @Test
    public void testMinimumSplitWeightRejectsInvalidValues()
    {
        assertInvalidMinimumSplitWeight(Double.NaN);
        assertInvalidMinimumSplitWeight(0.0);
        assertInvalidMinimumSplitWeight(-0.1);
        assertInvalidMinimumSplitWeight(1.1);
    }

    @Test
    public void testScanTagDefaultsAndValidValues()
    {
        assertThat(PaimonSessionProperties.getScanTagName(session(Map.of()))).isNull();
        assertThat(PaimonSessionProperties.getScanTagName(session(Map.of(SCAN_TAG, "tag-1"))))
                .isEqualTo("tag-1");
        assertThat(PaimonSessionProperties.getScanTagName(session(Map.of(SCAN_TAG, " tag-1 "))))
                .isEqualTo("tag-1");
    }

    @Test
    public void testPaimon15CreationTimeScanDefaultsAndValidValues()
    {
        assertThat(PaimonSessionProperties.getScanFileCreationTimeMillis(session(Map.of()))).isNull();
        assertThat(PaimonSessionProperties.getScanCreationTimeMillis(session(Map.of()))).isNull();
        assertThat(PaimonSessionProperties.getScanFileCreationTimeMillis(session(Map.of(SCAN_FILE_CREATION_TIME, 1000L))))
                .isEqualTo(1000L);
        assertThat(PaimonSessionProperties.getScanCreationTimeMillis(session(Map.of(SCAN_CREATION_TIME, 2000L))))
                .isEqualTo(2000L);
    }

    @Test
    public void testScanTagRejectsBlankValue()
    {
        assertThatThrownBy(() -> PaimonSessionProperties.getScanTagName(session(Map.of(SCAN_TAG, " "))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_SESSION_PROPERTY.toErrorCode());
                    assertThat(exception).hasMessage("%s must not be blank", SCAN_TAG);
                });
    }

    @Test
    public void testInsertExistingPartitionsBehaviorDefaultsAndCaseInsensitiveValues()
    {
        assertThat(PaimonSessionProperties.getInsertExistingPartitionsBehavior(session(Map.of())))
                .isEqualTo(PaimonSessionProperties.InsertExistingPartitionsBehavior.APPEND);
        assertThat(PaimonSessionProperties.getInsertExistingPartitionsBehavior(session(Map.of(
                INSERT_EXISTING_PARTITIONS_BEHAVIOR, "error"))))
                .isEqualTo(PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR);
        assertThat(PaimonSessionProperties.getInsertExistingPartitionsBehavior(session(Map.of(
                INSERT_EXISTING_PARTITIONS_BEHAVIOR, "OvErWrItE"))))
                .isEqualTo(PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE);
        assertThat(PaimonSessionProperties.getInsertExistingPartitionsBehavior(session(Map.of(
                INSERT_EXISTING_PARTITIONS_BEHAVIOR, " error "))))
                .isEqualTo(PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR);
    }

    private static void assertInvalidMinimumSplitWeight(double value)
    {
        assertThatThrownBy(() -> PaimonSessionProperties.getMinimumSplitWeight(session(Map.of(
                MINIMUM_SPLIT_WEIGHT, value))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_SESSION_PROPERTY.toErrorCode());
                    assertThat(exception).hasMessage(
                            "%s must be > 0 and <= 1.0: %s",
                            MINIMUM_SPLIT_WEIGHT,
                            value);
                });
    }

    private static ConnectorSession session(Map<String, Object> properties)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(properties)
                .build();
    }
}
