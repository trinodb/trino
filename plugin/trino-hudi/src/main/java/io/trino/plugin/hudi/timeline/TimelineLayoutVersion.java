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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class TimelineLayoutVersion
        implements Comparable<TimelineLayoutVersion>
{
    public static final Integer VERSION_0 = 0; // pre 0.5.1  version format
    public static final Integer VERSION_1 = 1; // current version with no renames

    private static final Integer CURRENT_VERSION = VERSION_1;
    public static final TimelineLayoutVersion CURRENT_LAYOUT_VERSION = new TimelineLayoutVersion(CURRENT_VERSION);

    private final Integer version;

    public TimelineLayoutVersion(Integer version)
    {
        checkArgument(version <= CURRENT_VERSION);
        checkArgument(version >= VERSION_0);
        this.version = version;
    }

    public Integer getVersion()
    {
        return version;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimelineLayoutVersion that = (TimelineLayoutVersion) o;
        return Objects.equals(version, that.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version);
    }

    @Override
    public int compareTo(TimelineLayoutVersion o)
    {
        return Integer.compare(version, o.version);
    }

    @Override
    public String toString()
    {
        return String.valueOf(version);
    }
}
