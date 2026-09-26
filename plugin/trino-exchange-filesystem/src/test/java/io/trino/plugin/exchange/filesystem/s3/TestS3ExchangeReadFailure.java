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
package io.trino.plugin.exchange.filesystem.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.CompletionException;

import static io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage.toReadFailure;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3ExchangeReadFailure
{
    private static final URI FILE = URI.create("s3://bucket/exchange/file");

    @Test
    public void testMissingObject()
    {
        assertThat(toReadFailure(NoSuchKeyException.builder().build(), FILE))
                .isInstanceOf(NoSuchFileException.class)
                .hasMessage(FILE.toString());
        assertThat(toReadFailure(S3Exception.builder().statusCode(404).build(), FILE))
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    public void testNestedCauseIsInspected()
    {
        RuntimeException failure = new CompletionException(NoSuchKeyException.builder().build());
        assertThat(toReadFailure(failure, FILE))
                .isInstanceOf(NoSuchFileException.class)
                .hasCause(failure);
    }

    @Test
    public void testOtherFailureStaysGeneric()
    {
        RuntimeException failure = S3Exception.builder().statusCode(503).build();
        assertThat(toReadFailure(failure, FILE))
                .isExactlyInstanceOf(IOException.class)
                .hasCause(failure);
    }
}
