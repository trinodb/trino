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
package io.trino.tracing;

import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;

import java.util.Map;

import static io.airlift.tracing.SpanSerialization.SpanDeserializer;
import static io.airlift.tracing.SpanSerialization.SpanSerializer;

public final class TracingJsonCodec
{
    private TracingJsonCodec() {}

    public static JsonCodecFactory tracingJsonCodecFactory()
    {
        return new JsonCodecFactory(tracingObjectMapperProvider());
    }

    public static ObjectMapperProvider tracingObjectMapperProvider()
    {
        return new ObjectMapperProvider()
                .withJsonSerializers(Map.of(Span.class, new SpanSerializer(OpenTelemetry.noop())))
                .withJsonDeserializers(Map.of(Span.class, new SpanDeserializer(OpenTelemetry.noop())));
    }
}
