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

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public final class Locations
{
    private Locations() {}

    public static String appendPath(String location, String path)
    {
        validateLocation(location);

        if (!location.endsWith("/")) {
            location += "/";
        }
        return location + path;
    }

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

    public static Optional<String> getFileName(String location)
    {
        validateLocation(location);

        int fileNameBeginIndex = location.lastIndexOf('/') + 1;
        if (fileNameBeginIndex == location.length()) {
            return Optional.empty();
        }
        return Optional.of(location.substring(fileNameBeginIndex));
    }

    private static void validateLocation(String location)
    {
        checkArgument(location.indexOf('?') < 0, "location contains a query string: %s", location);
        checkArgument(location.indexOf('#') < 0, "location contains a fragment: %s", location);
    }
}
