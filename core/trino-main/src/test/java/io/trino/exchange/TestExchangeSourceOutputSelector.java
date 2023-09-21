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
package io.trino.exchange;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.trino.server.SliceSerialization.SliceDeserializer;
import io.trino.server.SliceSerialization.SliceSerializer;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.exchange.ExchangeSourceOutputSelector.Selection.EXCLUDED;
import static io.trino.spi.exchange.ExchangeSourceOutputSelector.Selection.INCLUDED;
import static io.trino.spi.exchange.ExchangeSourceOutputSelector.Selection.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@TestInstance(PER_CLASS)
public class TestExchangeSourceOutputSelector
{
    private static final ExchangeId EXCHANGE_ID_1 = new ExchangeId("exchange_1");
    private static final ExchangeId EXCHANGE_ID_2 = new ExchangeId("exchange_2");

    private JsonCodec<ExchangeSourceOutputSelector> codec;

    @BeforeAll
    public void setup()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(Slice.class, new SliceSerializer()));
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Slice.class, new SliceDeserializer()));
        codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(ExchangeSourceOutputSelector.class);
    }

    @AfterAll
    public void tearDown()
    {
        codec = null;
    }

    @Test
    public void testEmpty()
    {
        {
            ExchangeSourceOutputSelector selector = serializeDeserialize(ExchangeSourceOutputSelector.builder(ImmutableSet.of(EXCHANGE_ID_1, EXCHANGE_ID_2))
                    .build());
            assertEquals(selector.getSelection(EXCHANGE_ID_1, 100, 1), UNKNOWN);
            assertEquals(selector.getSelection(EXCHANGE_ID_2, 21, 2), UNKNOWN);
            assertFalse(selector.isFinal());
        }

        {
            ExchangeSourceOutputSelector selector = serializeDeserialize(ExchangeSourceOutputSelector.builder(ImmutableSet.of(EXCHANGE_ID_1, EXCHANGE_ID_2))
                    .setPartitionCount(EXCHANGE_ID_1, 0)
                    .setPartitionCount(EXCHANGE_ID_2, 0)
                    .setFinal()
                    .build());
            assertTrue(selector.isFinal());
            // final selector should have selection set for all partitions
            assertThatThrownBy(() -> selector.getSelection(EXCHANGE_ID_1, 100, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("selection not found for exchangeId %s, taskPartitionId %s".formatted(EXCHANGE_ID_1, 100));
        }
    }

    @Test
    public void testNonFinal()
    {
        ExchangeSourceOutputSelector selector = serializeDeserialize(ExchangeSourceOutputSelector.builder(ImmutableSet.of(EXCHANGE_ID_1, EXCHANGE_ID_2))
                .include(EXCHANGE_ID_1, 21, 2)
                .exclude(EXCHANGE_ID_2, 100)
                .build());
        // ensure exchange id is taken into account
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 100, 1), UNKNOWN);
        assertEquals(selector.getSelection(EXCHANGE_ID_2, 100, 1), EXCLUDED);
        // all attempts of a given task must be excluded
        assertEquals(selector.getSelection(EXCHANGE_ID_2, 100, 2), EXCLUDED);
        // ensure exchange id is taken into account
        assertEquals(selector.getSelection(EXCHANGE_ID_2, 21, 2), UNKNOWN);
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 21, 2), INCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 21, 1), EXCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_2, 1, 2), UNKNOWN);
        assertEquals(selector.getSelection(EXCHANGE_ID_2, 200, 2), UNKNOWN);
        assertFalse(selector.isFinal());
    }

    @Test
    public void testFinal()
    {
        // partition count must be set
        assertThatThrownBy(() -> ExchangeSourceOutputSelector.builder(ImmutableSet.of(EXCHANGE_ID_1))
                .include(EXCHANGE_ID_1, 1, 2)
                .setFinal()
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("partition count is missing for exchange: %s".formatted(EXCHANGE_ID_1));

        ExchangeSourceOutputSelector selector = serializeDeserialize(ExchangeSourceOutputSelector.builder(ImmutableSet.of(EXCHANGE_ID_1, EXCHANGE_ID_2))
                .include(EXCHANGE_ID_1, 0, 1)
                .exclude(EXCHANGE_ID_1, 1)
                .include(EXCHANGE_ID_1, 2, 0)
                .exclude(EXCHANGE_ID_2, 0)
                .setPartitionCount(EXCHANGE_ID_1, 3)
                .setPartitionCount(EXCHANGE_ID_2, 1)
                .setFinal()
                .build());

        assertEquals(selector.getSelection(EXCHANGE_ID_1, 0, 1), INCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 0, 2), EXCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 1, 0), EXCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 1, 2), EXCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 2, 0), INCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_1, 2, 2), EXCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_2, 0, 1), EXCLUDED);
        assertEquals(selector.getSelection(EXCHANGE_ID_2, 0, 0), EXCLUDED);

        assertThatThrownBy(() -> selector.getSelection(EXCHANGE_ID_1, 100, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("selection not found for exchangeId %s, taskPartitionId %s".formatted(EXCHANGE_ID_1, 100));
    }

    @Test
    public void testBasicTransitions()
    {
        ExchangeSourceOutputSelector.Builder builder = ExchangeSourceOutputSelector.builder(ImmutableSet.of(EXCHANGE_ID_1, EXCHANGE_ID_2));
        ExchangeSourceOutputSelector partialVersion0 = builder.build();
        builder.include(EXCHANGE_ID_1, 0, 0);
        ExchangeSourceOutputSelector partialVersion1 = builder.build();
        builder.exclude(EXCHANGE_ID_1, 1);
        ExchangeSourceOutputSelector partialVersion2 = builder.build();
        builder.setPartitionCount(EXCHANGE_ID_1, 2);
        builder.setPartitionCount(EXCHANGE_ID_2, 0);
        builder.setFinal();
        ExchangeSourceOutputSelector finalVersion1 = builder.build();
        ExchangeSourceOutputSelector finalVersion2 = builder.build();

        // legitimate transitions
        partialVersion0.checkValidTransition(partialVersion1);
        partialVersion1.checkValidTransition(partialVersion2);
        partialVersion2.checkValidTransition(finalVersion1);

        assertThatThrownBy(() -> partialVersion1.checkValidTransition(partialVersion0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid transition to the same or an older version");
        assertThatThrownBy(() -> partialVersion2.checkValidTransition(partialVersion0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid transition to the same or an older version");
        assertThatThrownBy(() -> partialVersion2.checkValidTransition(partialVersion1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid transition to the same or an older version");
        assertThatThrownBy(() -> finalVersion2.checkValidTransition(finalVersion1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid transition to the same or an older version");
        assertThatThrownBy(() -> finalVersion2.checkValidTransition(partialVersion0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid transition to the same or an older version");
        assertThatThrownBy(() -> finalVersion1.checkValidTransition(finalVersion2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid transition from final selector");
    }

    @Test
    public void testIncompatibleTransitions()
    {
        ExchangeSourceOutputSelector.Builder builder = ExchangeSourceOutputSelector.builder(ImmutableSet.of(EXCHANGE_ID_1));
        builder.include(EXCHANGE_ID_1, 0, 0);
        assertThatThrownBy(() -> builder.include(EXCHANGE_ID_1, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("decision for partition 0 is already made: 0");
        assertThatThrownBy(() -> builder.exclude(EXCHANGE_ID_1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("decision for partition 0 is already made: 0");
    }

    private ExchangeSourceOutputSelector serializeDeserialize(ExchangeSourceOutputSelector selector)
    {
        return codec.fromJson(codec.toJson(selector));
    }
}
