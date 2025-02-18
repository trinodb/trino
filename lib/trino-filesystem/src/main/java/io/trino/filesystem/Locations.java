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
package io.trino.filesystem;

import java.util.regex.Pattern;

public final class Locations
{
    private static final Pattern S3_TABLES = Pattern.compile("s3://(?!.*/).*--table-s3");

    private Locations() {}

    /**
     * @deprecated use {@link Location#appendPath(String)} instead
     */
    @Deprecated
    public static String appendPath(String location, String path)
    {
        if (!location.endsWith("/")) {
            location += "/";
        }
        return location + path;
    }

    /**
     * Verifies whether the two provided directory location parameters point to the same actual location.
     */
    public static boolean areDirectoryLocationsEquivalent(Location leftLocation, Location rightLocation)
    {
        return leftLocation.equals(rightLocation) ||
                leftLocation.removeOneTrailingSlash().equals(rightLocation.removeOneTrailingSlash());
    }

    public static boolean isS3Tables(String location)
    {
        return S3_TABLES.matcher(location).matches();
    }
}
