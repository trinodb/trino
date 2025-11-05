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
package io.trino.server.protocol.spooling;

import io.trino.server.protocol.spooling.QueryDataEncoder.EncoderSelector;
import io.trino.server.protocol.spooling.encoding.JsonQueryDataEncoder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestPreferredQueryDataEncoderSelector
{
    @Test
    public void testNoEncoderWhenNoneIsMatching()
    {
        EncoderSelector selector = new PreferredQueryDataEncoderSelector(new QueryDataEncoders(new SpoolingEnabledConfig().setEnabled(true), Set.of(new JsonQueryDataEncoder.Factory())));

        assertThat(selector.select(List.of())).isEmpty();
        assertThat(selector.select(List.of("json+zstd"))).isEmpty();
    }

    @Test
    public void testNoEncoder()
    {
        EncoderSelector selector = new PreferredQueryDataEncoderSelector(new QueryDataEncoders(new SpoolingEnabledConfig().setEnabled(true), Set.of()));

        assertThat(selector.select(List.of())).isEmpty();
        assertThat(selector.select(List.of("json"))).isEmpty();
    }

    @Test
    public void testSingleMatchingEncoderIsPicked()
    {
        JsonQueryDataEncoder.Factory factory = new JsonQueryDataEncoder.Factory();
        EncoderSelector selector = new PreferredQueryDataEncoderSelector(new QueryDataEncoders(new SpoolingEnabledConfig().setEnabled(true), Set.of(factory)));

        assertThat(selector.select(List.of("json+zstd", "json"))).hasValue(factory);
    }

    @Test
    public void testSingleMatchingEncoderFromMultipleIsPicked()
    {
        JsonQueryDataEncoder.Factory factory = new JsonQueryDataEncoder.Factory();
        EncoderSelector selector = new PreferredQueryDataEncoderSelector(new QueryDataEncoders(new SpoolingEnabledConfig().setEnabled(true), Set.of(factory, new JsonQueryDataEncoder.ZstdFactory(new QueryDataEncodingConfig()))));

        assertThat(selector.select(List.of("protobuf", "json", "json+zstd"))).hasValue(factory);
    }

    @Test
    public void testSingleMatchingEncoderFromMultipleIsPickedInOrder()
    {
        JsonQueryDataEncoder.Factory factory = new JsonQueryDataEncoder.Factory();
        JsonQueryDataEncoder.ZstdFactory zstdFactory = new JsonQueryDataEncoder.ZstdFactory(new QueryDataEncodingConfig());

        EncoderSelector selector = new PreferredQueryDataEncoderSelector(new QueryDataEncoders(new SpoolingEnabledConfig().setEnabled(true), Set.of(factory, zstdFactory)));

        assertThat(selector.select(List.of("protobuf", "json+zstd", "json"))).hasValue(zstdFactory);
    }
}
