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
package io.trino.tests.product.hive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveVersion
{
    private static final Pattern HIVE_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+).*");

    private final int majorVersion;
    private final int minorVersion;
    private final int patchVersion;

    public HiveVersion(int majorVersion, int minorVersion, int patchVersion)
    {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.patchVersion = patchVersion;
    }

    public int getMajorVersion()
    {
        return majorVersion;
    }

    public int getMinorVersion()
    {
        return minorVersion;
    }

    public int getPatchVersion()
    {
        return patchVersion;
    }

    public static HiveVersion createFromString(String versionString)
    {
        Matcher matcher = HIVE_VERSION_PATTERN.matcher(versionString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Cannot parse Hive version " + versionString);
        }
        int majorVersion = Integer.parseInt(matcher.group(1));
        int minorVersion = Integer.parseInt(matcher.group(2));
        int patchVersion = Integer.parseInt(matcher.group(3));
        return new HiveVersion(majorVersion, minorVersion, patchVersion);
    }
}
