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

import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URLEncoder;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class HadoopPaths
{
    private HadoopPaths() {}

    public static Path hadoopPath(String path)
    {
        // hack to preserve the original path for S3 if necessary
        Path hadoopPath = new Path(path);
        if ("s3".equals(hadoopPath.toUri().getScheme()) && !path.equals(hadoopPath.toString())) {
            if (hadoopPath.toUri().getFragment() != null) {
                throw new IllegalArgumentException("Unexpected URI fragment in path: " + path);
            }
            URI uri = URI.create(path);
            return new Path(uri + "#" + URLEncoder.encode(uri.getPath(), UTF_8));
        }
        return hadoopPath;
    }
}
