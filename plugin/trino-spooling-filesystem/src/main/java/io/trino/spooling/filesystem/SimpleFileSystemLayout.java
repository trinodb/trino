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

import io.azam.ulidj.ULID;
import io.trino.filesystem.Location;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class SimpleFileSystemLayout
        implements FileSystemLayout
{
    @Override
    public Location location(Location rootLocation, FileSystemSpooledSegmentHandle segmentHandle)
    {
        return rootLocation.appendPath(segmentHandle.identifier() + "." + segmentHandle.encoding());
    }

    @Override
    public List<Location> searchPaths(Location rootLocation)
    {
        return List.of(rootLocation);
    }

    @Override
    public Optional<Instant> getExpiration(Location location)
    {
        String filename = location.fileName();
        int index = filename.indexOf(".");
        if (index == -1) {
            return Optional.empty(); // Not a segment
        }

        String uuid = filename.substring(0, index);
        if (!ULID.isValid(uuid)) {
            return Optional.empty();
        }
        return Optional.of(Instant.ofEpochMilli(ULID.getTimestamp(uuid)));
    }
}
