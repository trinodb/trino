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
package io.trino.client;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.spi.type.StandardTypes;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestClientTypeSignature
{
    public static final JsonCodec<ClientTypeSignature> CLIENT_TYPE_SIGNATURE_CODEC;

    static {
        CLIENT_TYPE_SIGNATURE_CODEC = new JsonCodecFactory().jsonCodec(ClientTypeSignature.class);
    }

    @Test
    public void testJsonRoundTrip()
    {
        ClientTypeSignature bigint = new ClientTypeSignature(StandardTypes.BIGINT);
        assertJsonRoundTrip(bigint);
        assertJsonRoundTrip(new ClientTypeSignature(
                "array",
                ImmutableList.of(ClientTypeSignatureParameter.ofType(bigint))));
        assertJsonRoundTrip(new ClientTypeSignature(
                "foo",
                ImmutableList.of(ClientTypeSignatureParameter.ofLong(42))));
        assertJsonRoundTrip(new ClientTypeSignature(
                "row",
                ImmutableList.of(
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("foo")), bigint)),
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("bar")), bigint)))));
    }

    @Test
    public void testStringSerialization()
    {
        ClientTypeSignature bigint = new ClientTypeSignature(StandardTypes.BIGINT);
        ClientTypeSignature varchar = new ClientTypeSignature(StandardTypes.VARCHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(50)));
        assertThat(bigint.toString()).isEqualTo("bigint");
        assertThat(varchar.toString()).isEqualTo("varchar(50)");
        ClientTypeSignature array = new ClientTypeSignature(StandardTypes.ARRAY, ImmutableList.of(ClientTypeSignatureParameter.ofType(new ClientTypeSignature(StandardTypes.BIGINT))));
        assertThat(array.toString()).isEqualTo("array(bigint)");
        ClientTypeSignature row = new ClientTypeSignature(
                StandardTypes.ROW,
                ImmutableList.of(
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("foo")), bigint)),
                        ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(Optional.of(new RowFieldName("bar")), bigint))));
        assertThat(row.toString()).isEqualTo("row(foo bigint,bar bigint)");
    }

    @Test
    public void testIntervalSerialization()
    {
        // a day-time interval has four arguments — start field, end field, leading precision, and
        // fractional-seconds precision — that render as the SQL qualifier
        assertThat(dayTime(2, 5, 9, 6).toString())
                .isEqualTo("interval day(9) to second(6)");
        assertThat(dayTime(5, 5, 13, 2).toString())
                .isEqualTo("interval second(13, 2)");
        assertThat(dayTime(3, 4, 10, 0).toString())
                .isEqualTo("interval hour(10) to minute");
        // a year-month interval has three arguments
        assertThat(yearMonth(0, 1, 2).toString())
                .isEqualTo("interval year(2) to month");
        assertThat(yearMonth(1, 1, 10).toString())
                .isEqualTo("interval month(10)");
    }

    private static ClientTypeSignature dayTime(long startField, long endField, long leadingPrecision, long fractionalPrecision)
    {
        return new ClientTypeSignature(ClientStandardTypes.INTERVAL_DAY_TO_SECOND, ImmutableList.of(
                ClientTypeSignatureParameter.ofLong(startField),
                ClientTypeSignatureParameter.ofLong(endField),
                ClientTypeSignatureParameter.ofLong(leadingPrecision),
                ClientTypeSignatureParameter.ofLong(fractionalPrecision)));
    }

    private static ClientTypeSignature yearMonth(long startField, long endField, long leadingPrecision)
    {
        return new ClientTypeSignature(ClientStandardTypes.INTERVAL_YEAR_TO_MONTH, ImmutableList.of(
                ClientTypeSignatureParameter.ofLong(startField),
                ClientTypeSignatureParameter.ofLong(endField),
                ClientTypeSignatureParameter.ofLong(leadingPrecision)));
    }

    private static void assertJsonRoundTrip(ClientTypeSignature signature)
    {
        String json = CLIENT_TYPE_SIGNATURE_CODEC.toJson(signature);
        ClientTypeSignature copy = CLIENT_TYPE_SIGNATURE_CODEC.fromJson(json);
        assertThat(copy).isEqualTo(signature);
    }
}
