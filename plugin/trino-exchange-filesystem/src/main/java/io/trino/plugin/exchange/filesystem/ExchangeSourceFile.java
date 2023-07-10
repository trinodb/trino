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
package io.trino.plugin.exchange.filesystem;

import com.google.errorprone.annotations.Immutable;
import io.trino.spi.exchange.ExchangeId;

import java.net.URI;

import static java.util.Objects.requireNonNull;

@Immutable
public class ExchangeSourceFile
{
    private final URI fileUri;
    private final long fileSize;
    private final ExchangeId exchangeId;
    private final int sourceTaskPartitionId;
    private final int sourceTaskAttemptId;

    public ExchangeSourceFile(URI fileUri, long fileSize, ExchangeId exchangeId, int sourceTaskPartitionId, int sourceTaskAttemptId)
    {
        this.fileUri = requireNonNull(fileUri, "fileUri is null");
        this.fileSize = fileSize;
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.sourceTaskPartitionId = sourceTaskPartitionId;
        this.sourceTaskAttemptId = sourceTaskAttemptId;
    }

    public URI getFileUri()
    {
        return fileUri;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public ExchangeId getExchangeId()
    {
        return exchangeId;
    }

    public int getSourceTaskPartitionId()
    {
        return sourceTaskPartitionId;
    }

    public int getSourceTaskAttemptId()
    {
        return sourceTaskAttemptId;
    }
}
