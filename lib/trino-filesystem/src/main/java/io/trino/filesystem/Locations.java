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

import static com.google.common.base.Preconditions.checkArgument;

public final class Locations
{
    private Locations() {}

    /**
     * @deprecated use {@link Location#appendPath(String)} instead
     */
    @Deprecated
    public static String appendPath(String location, String path)
    {
        validateLocation(location);

        if (!location.endsWith("/")) {
            location += "/";
        }
        return location + path;
    }

    /**
     * @deprecated use {@link Location#parentDirectory()} instead
     */
    @Deprecated
    public static String getParent(String location)
    {
        validateLocation(location);

        int lastIndexOfSlash = location.lastIndexOf('/');
        if (lastIndexOfSlash > 0) {
            String parent = location.substring(0, lastIndexOfSlash);
            if (!parent.endsWith("/") && !parent.endsWith(":")) {
                return parent;
            }
        }
        throw new IllegalArgumentException("Location does not have parent: " + location);
    }

    /**
     * @deprecated use {@link Location#fileName()} instead
     */
    @Deprecated
    public static String getFileName(String location)
    {
        validateLocation(location);

        return location.substring(location.lastIndexOf('/') + 1);
    }

    private static void validateLocation(String location)
    {
        checkArgument(location.indexOf('?') < 0, "location contains a query string: %s", location);
        checkArgument(location.indexOf('#') < 0, "location contains a fragment: %s", location);
    }

    /**
     * Verifies whether the two provided directory location parameters point to the same actual location.
     */
    public static boolean areDirectoryLocationsEquivalent(Location leftLocation, Location rightLocation)
    {
        return leftLocation.equals(rightLocation) ||
                leftLocation.removeOneTrailingSlash().equals(rightLocation.removeOneTrailingSlash());
    }
}
