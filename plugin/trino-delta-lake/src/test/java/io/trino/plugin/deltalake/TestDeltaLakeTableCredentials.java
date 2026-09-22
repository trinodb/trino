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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeTableCredentials
{
    private static final JsonCodec<DeltaLakeTableCredentials> CODEC = JsonCodec.jsonCodec(DeltaLakeTableCredentials.class);

    @Test
    public void testRoundTrip()
    {
        DeltaLakeTableCredentials credentials = new DeltaLakeTableCredentials(
                new VendedCredentialsHandle(true, true, "s3://bucket/table"),
                ImmutableMap.of("access-key", "key", "secret-key", "secret"));

        assertThat(CODEC.fromJson(CODEC.toJson(credentials))).isEqualTo(credentials);
    }

    @Test
    public void testOfResolvesFileSystemCredentials()
    {
        VendedCredentialsHandle handle = new VendedCredentialsHandle(true, true, "s3://bucket/table");
        DeltaLakeTableCredentials credentials = DeltaLakeTableCredentials.of(handle, new TestingFileSystemCredentials());

        assertThat(credentials.extraCredentials()).isEqualTo(ImmutableMap.of("access-key", "key"));
        assertThat(CODEC.fromJson(CODEC.toJson(credentials))).isEqualTo(credentials);
    }

    private static class TestingFileSystemCredentials
            implements FileSystemCredentials
    {
        @Override
        public Map<String, String> asExtraCredentials()
        {
            return ImmutableMap.of("access-key", "key");
        }

        @Override
        public boolean isValid()
        {
            return true;
        }
    }
}
