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
package org.apache.iceberg;

import org.apache.iceberg.io.FileIO;

import java.util.List;

public class IcebergManifestUtils
{
    private IcebergManifestUtils() {}

    public static List<ManifestFile> read(FileIO fileIO, String manifestListLocation)
    {
        // Avoid using snapshot.allManifests() when processing multiple snapshots,
        // as each Snapshot instance internally caches `org.apache.iceberg.BaseSnapshot.allManifests`
        // and leads to high memory usage
        return ManifestLists.read(fileIO.newInputFile(manifestListLocation));
    }
}
