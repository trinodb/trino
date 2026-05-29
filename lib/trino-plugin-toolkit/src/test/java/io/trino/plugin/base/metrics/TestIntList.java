package io.trino.plugin.base.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.spi.metrics.Metrics;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIntList
{
    private final JsonCodec<IntList> codec = jsonCodec(IntList.class);
    private final JsonCodec<Metrics> metricsCodec = jsonCodec(Metrics.class);

    @Test
    void testRoundTrip()
    {
        IntList expected = new IntList(ImmutableList.of(1));

        String json = codec.toJson(expected);
        IntList actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testRoundTripViaMetrics()
    {
        Metrics metrics = new Metrics(ImmutableMap.of("projected_fields", new IntList(ImmutableList.of(1, 2, 3))));
        String json = metricsCodec.toJson(metrics);
        Metrics actual = metricsCodec.fromJson(json);
        assertThat(actual).isEqualTo(metrics);
    }
}
