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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.MetricsBuilder;
import io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage.S3ExchangeStorageReader;
import io.trino.spi.exchange.ExchangeId;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage.toReadFailure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3ExchangeReadFailure
{
    private static final URI FILE = URI.create("s3://bucket/exchange/file");
    private static final URI FIRST_FILE = URI.create("s3://bucket/exchange/0_0.data");
    private static final URI LAST_FILE = URI.create("s3://bucket/exchange/1_0.data");
    private static final ExchangeId EXCHANGE_ID = new ExchangeId("exchange");

    @Test
    public void testReaderMissingLastFileOfBufferFill()
    {
        // Both files fit into a single buffer fill, which consumes all source files before the requests complete
        try (S3ExchangeStorageReader reader = createReader(s3Client(LAST_FILE, notFound()))) {
            assertThatThrownBy(reader::read)
                    .isInstanceOf(NoSuchFileException.class)
                    .hasMessage(LAST_FILE.toString());
        }
    }

    @Test
    public void testReaderMissingEarlierFileOfBufferFill()
    {
        try (S3ExchangeStorageReader reader = createReader(s3Client(FIRST_FILE, notFound()))) {
            assertThatThrownBy(reader::read)
                    .isInstanceOf(NoSuchFileException.class)
                    .hasMessage(FIRST_FILE.toString());
        }
    }

    @Test
    public void testReaderOtherFailureStaysGeneric()
    {
        S3Exception failure = (S3Exception) S3Exception.builder().statusCode(503).build();
        try (S3ExchangeStorageReader reader = createReader(s3Client(LAST_FILE, failure))) {
            assertThatThrownBy(reader::read)
                    .isExactlyInstanceOf(IOException.class)
                    .hasCause(failure);
        }
    }

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

    private static S3ExchangeStorageReader createReader(S3AsyncClient s3Client)
    {
        List<ExchangeSourceFile> sourceFiles = ImmutableList.of(
                new ExchangeSourceFile(FIRST_FILE, 16, EXCHANGE_ID, 0, 0),
                new ExchangeSourceFile(LAST_FILE, 16, EXCHANGE_ID, 1, 0));
        return new S3ExchangeStorageReader(new S3FileSystemExchangeStorageStats(), _ -> s3Client, 1024, sourceFiles, new MetricsBuilder(), 1024);
    }

    private static NoSuchKeyException notFound()
    {
        return (NoSuchKeyException) NoSuchKeyException.builder().statusCode(404).build();
    }

    /**
     * Fails requests for {@code failingFile}, requests for other files never complete
     */
    private static S3AsyncClient s3Client(URI failingFile, Exception failure)
    {
        String failingKey = failingFile.getPath().substring(1);
        return new S3AsyncClient()
        {
            @Override
            public <ReturnT> CompletableFuture<ReturnT> getObject(GetObjectRequest request, AsyncResponseTransformer<GetObjectResponse, ReturnT> transformer)
            {
                if (request.key().equals(failingKey)) {
                    return CompletableFuture.failedFuture(failure);
                }
                return new CompletableFuture<>();
            }

            @Override
            public String serviceName()
            {
                return SERVICE_NAME;
            }

            @Override
            public void close() {}
        };
    }
}
