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
package io.trino.filesystem.alluxio;

import alluxio.AlluxioURI;
import io.trino.filesystem.Location;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AlluxioUtils
{
    private AlluxioUtils() {}

    public static Location convertToLocation(String path, String mountRoot)
    {
        requireNonNull(path, "path is null");

        if (path.isEmpty()) {
            return Location.of("");
        }
        if (path.startsWith("alluxio://")) {
            return Location.of(path);
        }

        String schema = "alluxio://";
        if (path.startsWith("/")) {
            while (path.startsWith("/")) {
                path = path.substring(1);
            }
        }
        String mountRootWithSlash = mountRoot;
        if (!mountRoot.endsWith("/")) {
            mountRootWithSlash = mountRoot + "/";
        }
        return Location.of(schema + mountRootWithSlash + path);
    }

    public static String getAlluxioBase(String path)
    {
        requireNonNull(path, "path is null");
        if (!path.startsWith("alluxio://")) {
            throw new IllegalArgumentException("path is not an alluxio://");
        }
        int index = path.indexOf('/', "alluxio://".length());
        return path.substring(0, index);
    }

    public static String simplifyPath(String path)
    {
        // Use a deque to store the path components
        Deque<String> deque = new ArrayDeque<>();
        String[] segments = path.split("/");

        for (String segment : segments) {
            if (segment.isEmpty() || segment.equals(".")) {
                // Ignore empty and current directory segments
                continue;
            }
            if (segment.equals("..")) {
                // If there's a valid directory to go back to, remove it
                if (!deque.isEmpty()) {
                    deque.pollLast();
                }
            }
            else {
                // Add the directory to the deque
                deque.offerLast(segment);
            }
        }

        // Build the final simplified path from the deque
        StringBuilder simplifiedPath = new StringBuilder();
        for (String dir : deque) {
            simplifiedPath.append(dir).append("/");
        }

        // Retain trailing slash if it was in the original path
        if (!path.endsWith("/") && simplifiedPath.length() > 0) {
            simplifiedPath.setLength(simplifiedPath.length() - 1);
        }

        return simplifiedPath.length() == 0 ? "" : simplifiedPath.toString();
    }

    public static AlluxioURI convertToAlluxioURI(Location location, String mountRoot)
    {
        Optional<String> scheme = location.scheme();
        if (scheme.isPresent()) {
            if (!scheme.get().equals("alluxio")) {
                return new AlluxioURI(location.toString());
            }
        }
        String path = location.path();
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (!mountRoot.endsWith("/")) {
            mountRoot = mountRoot + "/";
        }
        return new AlluxioURI(mountRoot + path);
    }
}
