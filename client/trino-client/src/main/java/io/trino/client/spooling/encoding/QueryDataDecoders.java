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
package io.trino.client.spooling.encoding;

import com.google.common.collect.ImmutableList;
import io.trino.client.QueryDataDecoder.Factory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

public class QueryDataDecoders
{
    private static final List<Factory> decoders = ImmutableList.of(
            new JsonQueryDataDecoder.Factory(),
            new JsonQueryDataDecoder.ZstdFactory(),
            new JsonQueryDataDecoder.Lz4Factory());

    private static final Map<String, Factory> encodingMap = factoriesMap();

    private QueryDataDecoders() {}

    public static Factory get(String encoding)
    {
        if (!encodingMap.containsKey(encoding)) {
            throw new IllegalArgumentException("Unknown encoding: " + encoding);
        }

        Factory factory = encodingMap.get(encoding);
        verify(factory.encoding().equals(encoding), "Factory has wrong encoding, expected %s, got %s", encoding, factory.encoding());
        return factory;
    }

    public static boolean exists(String encoding)
    {
        return encodingMap.containsKey(encoding);
    }

    public static Set<String> getSupportedEncodings()
    {
        return encodingMap.keySet();
    }

    private static Map<String, Factory> factoriesMap()
    {
        return decoders.stream()
                .collect(toImmutableMap(Factory::encoding, identity()));
    }
}
