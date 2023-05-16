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
package io.trino.filesystem.hdfs;

import com.google.common.base.VerifyException;
import io.trino.filesystem.Location;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

public final class HadoopPaths
{
    private HadoopPaths() {}

    public static Path hadoopPath(Location location)
    {
        // hack to preserve the original path for S3 if necessary
        String path = location.toString();
        Path hadoopPath = new Path(path);
        if ("s3".equals(hadoopPath.toUri().getScheme()) && !path.equals(hadoopPath.toString())) {
            return new Path(toPathEncodedUri(location));
        }
        return hadoopPath;
    }

    private static URI toPathEncodedUri(Location location)
    {
        try {
            return new URI(
                    location.scheme().orElse(null),
                    location.host().orElse(null),
                    "/" + location.path(),
                    location.path());
        }
        catch (URISyntaxException e) {
            throw new VerifyException("Failed to convert location to URI: " + location, e);
        }
    }
}
