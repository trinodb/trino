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
package io.trino.decoder.protobuf;

import org.testng.annotations.DataProvider;

import java.time.LocalDateTime;

import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.lang.Math.PI;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

public class ProtobufDataProviders
{
    @DataProvider
    public Object[][] allTypesDataProvider()
    {
        return new Object[][] {
                {
                        "Trino",
                        1,
                        493857959588286460L,
                        PI,
                        3.14f,
                        true,
                        "ONE",
                        sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")),
                        "X'65683F'".getBytes(UTF_8)
                },
                {
                        range(0, 5000)
                                .mapToObj(Integer::toString)
                                .collect(joining(", ")),
                        Integer.MAX_VALUE,
                        Long.MIN_VALUE,
                        Double.MAX_VALUE,
                        Float.MIN_VALUE,
                        false,
                        "ZERO",
                        sqlTimestampOf(3, LocalDateTime.parse("1856-01-12T05:25:14.456")),
                        new byte[0]
                },
                {
                        range(5000, 10000)
                                .mapToObj(Integer::toString)
                                .collect(joining(", ")),
                        Integer.MIN_VALUE,
                        Long.MAX_VALUE,
                        Double.NaN,
                        Float.NEGATIVE_INFINITY,
                        false,
                        "ZERO",
                        sqlTimestampOf(3, LocalDateTime.parse("0001-01-01T00:00:00.923")),
                        "X'65683F'".getBytes(UTF_8)
                }
        };
    }
}
