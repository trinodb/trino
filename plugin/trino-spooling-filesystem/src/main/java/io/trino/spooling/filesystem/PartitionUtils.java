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
package io.trino.spooling.filesystem;

import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.primitives.Shorts.checkedCast;
import static java.lang.Math.abs;
import static java.lang.Short.parseShort;

public class PartitionUtils
{
    private PartitionUtils() {}

    public static short random(Random random, int partitions)
    {
        return checkedCast(abs(random.nextInt() % partitions));
    }

    public static String partitionToString(int partition)
    {
        return String.format("%1$02X", checkedCast(partition));
    }

    public static short partitionFromString(String partition)
    {
        return parseShort(partition, 16);
    }

    public static Stream<String> listPartitions(int partitions)
    {
        return IntStream.range(0, partitions)
                .mapToObj(PartitionUtils::partitionToString);
    }
}
