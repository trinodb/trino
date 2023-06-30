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
package io.trino.plugin.hudi.timeline;

import io.trino.plugin.hudi.model.HudiInstant;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TimelineLayout
{
    private static final Map<TimelineLayoutVersion, TimelineLayout> LAYOUT_MAP = new HashMap<>();

    static {
        LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_0), new TimelineLayout.TimelineLayoutV0());
        LAYOUT_MAP.put(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_1), new TimelineLayout.TimelineLayoutV1());
    }

    public static TimelineLayout getLayout(TimelineLayoutVersion version)
    {
        return LAYOUT_MAP.get(version);
    }

    public abstract Stream<HudiInstant> filterHoodieInstants(Stream<HudiInstant> instantStream);

    private static class TimelineLayoutV0
            extends TimelineLayout
    {
        @Override
        public Stream<HudiInstant> filterHoodieInstants(Stream<HudiInstant> instantStream)
        {
            return instantStream;
        }
    }

    private static class TimelineLayoutV1
            extends TimelineLayout
    {
        @Override
        public Stream<HudiInstant> filterHoodieInstants(Stream<HudiInstant> instantStream)
        {
            return instantStream.collect(Collectors.groupingBy(instant -> Map.entry(instant.getTimestamp(),
                            HudiInstant.getComparableAction(instant.getAction()))))
                    .values()
                    .stream()
                    .map(hoodieInstants ->
                            hoodieInstants.stream().reduce((x, y) -> {
                                // Pick the one with the highest state
                                if (x.getState().compareTo(y.getState()) >= 0) {
                                    return x;
                                }
                                return y;
                            }).get());
        }
    }
}
