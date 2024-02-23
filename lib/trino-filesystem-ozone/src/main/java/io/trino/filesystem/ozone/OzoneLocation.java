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
package io.trino.filesystem.ozone;

import io.trino.filesystem.Location;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// https://ozone.apache.org/docs/1.4.0/interface.html
// ofs supports operations across all volumes and buckets, same as o3:// ?
// ofs://om1/
// ofs://om3:9862/
// ofs://omservice/
// ofs://omservice/volume1/
// ofs://omservice/volume1/bucket1/
// ofs://omservice/volume1/bucket1/dir1
// ofs://omservice/volume1/bucket1/dir1/key1
// ofs://omservice/tmp/
// ofs://omservice/tmp/key1
//
// o3fs supports operations only at a single bucket
// o3fs://bucket.volume.om-host.example.com/key
// o3fs://bucket.volume.om-host.example.com:6789/key
// <property>
//  <name>fs.defaultFS</name>
//  <value>o3fs://bucket.volume</value>
// </property>
// o3fs://key1
// o3fs://dir1/key1
// o3fs://bucket.volume.om-host.example.com:5678/key

// (schema)(server:port)/volume/bucket/key
// schema: 'o3://'
// /volume1/bucket1/dir1/key1
record OzoneLocation(Location location)
{
    OzoneLocation
    {
        requireNonNull(location, "location is null");
        checkArgument(location.scheme().isPresent(), "No scheme for Ozone location: %s", location);
        checkArgument(Set.of("o3", "ofs", "o3fs").contains(location.scheme().get()), "Wrong scheme for Ozone location: %s", location);
        checkArgument(location.host().isPresent(), "No bucket for Ozone location: %s", location);
        checkArgument(location.userInfo().isEmpty(), "Ozone location contains user info: %s", location);
        checkArgument(location.port().isEmpty(), "Ozone location contains port: %s", location);
    }

    public String scheme()
    {
        return location.scheme().orElseThrow();
    }

    public String volume()
    {
        return "s3v";
    }

    public String bucket()
    {
        return "bucket1";
    }

    public String key()
    {
        return location.path();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    public Location baseLocation()
    {
        return Location.of("%s://%s/".formatted(scheme(), bucket()));
    }
}
