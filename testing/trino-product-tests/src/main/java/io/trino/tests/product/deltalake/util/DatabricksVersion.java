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
package io.trino.tests.product.deltalake.util;

import com.google.common.collect.ComparisonChain;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record DatabricksVersion(int majorVersion, int minorVersion)
        implements Comparable<DatabricksVersion>
{
    public static final DatabricksVersion DATABRICKS_122_RUNTIME_VERSION = new DatabricksVersion(12, 2);
    public static final DatabricksVersion DATABRICKS_113_RUNTIME_VERSION = new DatabricksVersion(11, 3);
    public static final DatabricksVersion DATABRICKS_104_RUNTIME_VERSION = new DatabricksVersion(10, 4);
    public static final DatabricksVersion DATABRICKS_91_RUNTIME_VERSION = new DatabricksVersion(9, 1);

    private static final Pattern DATABRICKS_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)");

    public boolean isAtLeast(DatabricksVersion version)
    {
        return compareTo(version) >= 0;
    }

    public boolean isOlderThan(DatabricksVersion version)
    {
        return compareTo(version) < 0;
    }

    @Override
    public int compareTo(DatabricksVersion other)
    {
        return ComparisonChain.start()
                .compare(majorVersion, other.majorVersion)
                .compare(minorVersion, other.minorVersion)
                .result();
    }

    public static DatabricksVersion parse(String versionString)
    {
        Matcher matcher = DATABRICKS_VERSION_PATTERN.matcher(versionString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Cannot parse Databricks version " + versionString);
        }
        int majorVersion = Integer.parseInt(matcher.group(1));
        int minorVersion = Integer.parseInt(matcher.group(2));
        return new DatabricksVersion(majorVersion, minorVersion);
    }
}
