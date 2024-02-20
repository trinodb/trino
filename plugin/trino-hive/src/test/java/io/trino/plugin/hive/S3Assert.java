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

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class S3Assert
{
    private final AmazonS3 s3;
    private final String path;
    private final String bucket;
    private final String key;

    public S3Assert(AmazonS3 s3, String path)
    {
        this(
                s3,
                path,
                regexpExtract(path, "s3://([^/]+)/(.+)", 1),
                regexpExtract(path, "s3://([^/]+)/(.+)", 2));
    }

    public S3Assert(AmazonS3 s3, String path, String bucket, String key)
    {
        this.s3 = requireNonNull(s3, "s3 is null");
        this.path = requireNonNull(path, "path is null");
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.key = requireNonNull(key, "key is null");
    }

    public static AssertProvider<S3Assert> s3Path(AmazonS3 s3, String path)
    {
        return () -> new S3Assert(s3, path);
    }

    private static String regexpExtract(String input, String regex, int group)
    {
        Matcher matcher = Pattern.compile(regex).matcher(input);
        verify(matcher.matches(), "Does not match [%s]: [%s]", matcher.pattern(), input);
        return matcher.group(group);
    }

    @CanIgnoreReturnValue
    public S3Assert exists()
    {
        assertThat(s3.doesObjectExist(bucket, key)).as("Existence of %s", path)
                .isTrue();
        return this;
    }
}
