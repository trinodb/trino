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

import com.google.common.base.Splitter;
import io.trino.filesystem.Location;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// https://ozone.apache.org/docs/1.4.0/interface.html
// ofs://om1/
//ofs://om3:9862/
//ofs://omservice/
//ofs://omservice/volume1/
//ofs://omservice/volume1/bucket1/
//ofs://omservice/volume1/bucket1/dir1
//ofs://omservice/volume1/bucket1/dir1/key1
//
//ofs://omservice/tmp/
//ofs://omservice/tmp/key1
//ofs://om-host.example.com
//<property>
//  <name>fs.defaultFS</name>
//  <value>ofs://omservice</value>
//</property>
//ozone fs -mkdir -p /volume1/bucket1
//ozone fs -mkdir -p ofs://omservice/volume1/bucket1/dir1/
//---
//<property>
//  <name>fs.defaultFS</name>
//  <value>o3fs://bucket.volume</value>
//</property>
//o3fs://bucket.volume.om-host.example.com:5678/key
//
//hdfs dfs -ls o3fs://bucket.volume.om-host.example.com/key
//hdfs dfs -ls o3fs://bucket.volume.om-host.example.com:6789/key

// TODO: Location work well with the above syntax
// Now only support: o3://volume/bucket/key
record OzoneLocation(Location location)
{
    OzoneLocation
    {
        requireNonNull(location, "location is null");
        checkArgument(location.scheme().isPresent(), "No scheme for Ozone location: %s", location);
        // TODO: support ofs, o3fs or not
        // checkArgument(Set.of("o3", "ofs", "o3fs").contains(location.scheme().get()), "Wrong scheme for Ozone location: %s", location);
        checkArgument("o3".equals(location.scheme().get()), "Wrong scheme for Ozone location: %s", location);
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
        return location.host().orElseThrow();
    }

    public String bucket()
    {
        Splitter splitter = Splitter.on('/').limit(2);
        return splitter.splitToList(location.path()).getFirst();
    }

    public String key()
    {
        Splitter splitter = Splitter.on('/').limit(2);
        return splitter.splitToList(location.path()).getLast();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    public Location baseLocation()
    {
        return Location.of("%s://%s/%s/".formatted(scheme(), volume(), bucket()));
    }
}
