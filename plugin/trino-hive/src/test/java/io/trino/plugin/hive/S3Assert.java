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
package io.trino.plugin.hive;

import com.amazonaws.services.s3.AmazonS3;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.util.CanIgnoreReturnValue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class S3Assert
{
    private static final Pattern S3_LOCATION_PATTERN = Pattern.compile("s3://(?<bucket>[^/]+)/(?<key>.+)");

    private final AmazonS3 s3;
    private final String path;
    private final String bucket;
    private final String key;

    private S3Assert(AmazonS3 s3, String path)
    {
        this.s3 = requireNonNull(s3, "s3 is null");
        this.path = requireNonNull(path, "path is null");
        Matcher matcher = S3_LOCATION_PATTERN.matcher(path);
        checkArgument(matcher.matches(), "Invalid S3 location: %s", path);
        this.bucket = matcher.group("bucket");
        this.key = matcher.group("key");
    }

    public static AssertProvider<S3Assert> s3Path(AmazonS3 s3, String path)
    {
        return () -> new S3Assert(s3, path);
    }

    @CanIgnoreReturnValue
    public S3Assert exists()
    {
        assertThat(s3.doesObjectExist(bucket, key)).as("Existence of %s", path)
                .isTrue();
        return this;
    }
}
