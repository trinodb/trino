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

import org.assertj.core.api.AssertProvider;
import org.assertj.core.util.CanIgnoreReturnValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Fail.fail;

public class S3Assert
{
    private static final Pattern S3_LOCATION_PATTERN = Pattern.compile("s3://(?<bucket>[^/]+)/(?<key>.+)");

    private final S3Client s3;
    private final String bucket;
    private final String key;

    private S3Assert(S3Client s3, String path)
    {
        this.s3 = requireNonNull(s3, "s3 is null");
        requireNonNull(path, "path is null");
        Matcher matcher = S3_LOCATION_PATTERN.matcher(path);
        checkArgument(matcher.matches(), "Invalid S3 location: %s", path);
        this.bucket = matcher.group("bucket");
        this.key = matcher.group("key");
    }

    public static AssertProvider<S3Assert> s3Path(S3Client s3, String path)
    {
        return () -> new S3Assert(s3, path);
    }

    @CanIgnoreReturnValue
    public S3Assert exists()
    {
        try {
            s3.headObject(request -> request.bucket(bucket).key(key));
        }
        catch (NoSuchKeyException _) {
            fail("Specified Object bucket=[%s] key=[%s] does not exist", bucket, key);
        }
        return this;
    }
}
