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
package io.trino.parquet.writer.valuewriter;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.io.api.Binary;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * Based on org.apache.parquet.column.values.fallback.FallbackValuesWriter
 */
public class DictionaryFallbackValuesWriter
        extends ValuesWriter
{
    private final ValuesWriter fallBackWriter;

    private boolean fellBackAlready;
    private ValuesWriter currentWriter;
    @Nullable
    private DictionaryValuesWriter initialWriter;
    private boolean initialUsedAndHadDictionary;
    /* size of raw data, even if dictionary is used, it will not have effect on raw data size, it is used to decide
     * if fall back to plain encoding is better by comparing rawDataByteSize with Encoded data size
     * It's also used in getBufferedSize, so the page will be written based on raw data size
     */
    private long rawDataByteSize;
    // indicates if this is the first page being processed
    private boolean firstPage = true;

    public DictionaryFallbackValuesWriter(DictionaryValuesWriter initialWriter, ValuesWriter fallBackWriter)
    {
        super();
        this.initialWriter = initialWriter;
        this.fallBackWriter = fallBackWriter;
        this.currentWriter = initialWriter;
    }

    @Override
    public long getBufferedSize()
    {
        // use raw data size to decide if we want to flush the page
        // so the actual size of the page written could be much more smaller
        // due to dictionary encoding. This prevents page being too big when fallback happens.
        return rawDataByteSize;
    }

    @Override
    public BytesInput getBytes()
    {
        if (!fellBackAlready && firstPage) {
            // we use the first page to decide if we're going to use this encoding
            BytesInput bytes = initialWriter.getBytes();
            if (!initialWriter.isCompressionSatisfying(rawDataByteSize, bytes.size())) {
                fallBack();
                // Since fallback happened on first page itself, we can drop the contents of initialWriter
                initialWriter.close();
                initialWriter = null;
                verify(!initialUsedAndHadDictionary, "initialUsedAndHadDictionary should be false when falling back to PLAIN in first page");
            }
            else {
                return bytes;
            }
        }
        return currentWriter.getBytes();
    }

    @Override
    public Encoding getEncoding()
    {
        Encoding encoding = currentWriter.getEncoding();
        if (!fellBackAlready && !initialUsedAndHadDictionary) {
            initialUsedAndHadDictionary = encoding.usesDictionary();
        }
        return encoding;
    }

    @Override
    public void reset()
    {
        rawDataByteSize = 0;
        firstPage = false;
        currentWriter.reset();
    }

    @Override
    public void close()
    {
        if (initialWriter != null) {
            initialWriter.close();
        }
        fallBackWriter.close();
    }

    @Override
    public DictionaryPage toDictPageAndClose()
    {
        if (initialUsedAndHadDictionary) {
            return initialWriter.toDictPageAndClose();
        }
        else {
            return currentWriter.toDictPageAndClose();
        }
    }

    @Override
    public void resetDictionary()
    {
        if (initialUsedAndHadDictionary) {
            initialWriter.resetDictionary();
        }
        else {
            currentWriter.resetDictionary();
        }
        currentWriter = initialWriter;
        fellBackAlready = false;
        initialUsedAndHadDictionary = false;
        firstPage = true;
    }

    @Override
    public long getAllocatedSize()
    {
        return fallBackWriter.getAllocatedSize() + (initialWriter != null ? initialWriter.getAllocatedSize() : 0);
    }

    @Override
    public String memUsageString(String prefix)
    {
        return String.format(
                "%s FallbackValuesWriter{\n"
                        + "%s\n"
                        + "%s\n"
                        + "%s}\n",
                prefix,
                initialWriter != null ? initialWriter.memUsageString(prefix + " initial:") : "",
                fallBackWriter.memUsageString(prefix + " fallback:"),
                prefix);
    }

    // passthrough writing the value
    @Override
    public void writeByte(int value)
    {
        rawDataByteSize += Byte.BYTES;
        currentWriter.writeByte(value);
        checkFallback();
    }

    @Override
    public void writeBytes(Binary value)
    {
        // For raw data, length(4 bytes int) is stored, followed by the binary content itself
        rawDataByteSize += value.length() + Integer.BYTES;
        currentWriter.writeBytes(value);
        checkFallback();
    }

    @Override
    public void writeInteger(int value)
    {
        rawDataByteSize += Integer.BYTES;
        currentWriter.writeInteger(value);
        checkFallback();
    }

    @Override
    public void writeLong(long value)
    {
        rawDataByteSize += Long.BYTES;
        currentWriter.writeLong(value);
        checkFallback();
    }

    @Override
    public void writeFloat(float value)
    {
        rawDataByteSize += Float.BYTES;
        currentWriter.writeFloat(value);
        checkFallback();
    }

    @Override
    public void writeDouble(double value)
    {
        rawDataByteSize += Double.BYTES;
        currentWriter.writeDouble(value);
        checkFallback();
    }

    @VisibleForTesting
    public DictionaryValuesWriter getInitialWriter()
    {
        return requireNonNull(initialWriter, "initialWriter is null");
    }

    @VisibleForTesting
    public ValuesWriter getFallBackWriter()
    {
        return fallBackWriter;
    }

    private void checkFallback()
    {
        if (!fellBackAlready && initialWriter.shouldFallBack()) {
            fallBack();
        }
    }

    private void fallBack()
    {
        fellBackAlready = true;
        initialWriter.fallBackAllValuesTo(fallBackWriter);
        currentWriter = fallBackWriter;
    }
}
