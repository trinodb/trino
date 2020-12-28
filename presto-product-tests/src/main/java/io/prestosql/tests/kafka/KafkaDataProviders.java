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
package io.prestosql.tests.kafka;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.DataProvider;

import java.util.List;
import java.util.stream.Collector;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public final class KafkaDataProviders
{
    private KafkaDataProviders() {}

    @DataProvider
    public static Object[][] kafkaCatalogs()
            throws Exception
    {
        ByteSource byteSource = Resources.asByteSource(Resources.getResource("io/prestosql/tests/kafka_catalogs.json"));
        List<KafkaCatalog> kafkaCatalogs = JsonCodec.listJsonCodec(KafkaCatalog.class).fromJson(byteSource.read());
        return kafkaCatalogs.stream()
                .collect(toDataProvider());
    }

    private static <T> Collector<T, ?, Object[][]> toDataProvider()
    {
        return collectingAndThen(
                mapping(
                        value -> new Object[] {value},
                        toList()),
                list -> list.toArray(new Object[][] {}));
    }
}
