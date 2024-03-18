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
package io.trino.plugin.varada.storage.write;

import io.airlift.log.Logger;
import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.warmup.exceptions.MaxRowsException;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;

import java.util.List;

public class VaradaPageSink
        implements PageSink
{
    private static final Logger logger = Logger.get(VaradaPageSink.class);
    private final StorageWriterService storageWriterService;
    private final StorageWriterSplitConfiguration storageWriterSplitConfiguration;
    private boolean writerOpened; // represents a writer(native) open
    private WarmUpElement abortedWarmupElement;

    private StorageWriterContext storageWriterContext;

    public VaradaPageSink(StorageWriterService storageWriterService, StorageWriterSplitConfiguration storageWriterSplitConfiguration)
    {
        this.storageWriterService = storageWriterService;
        this.storageWriterSplitConfiguration = storageWriterSplitConfiguration;
    }

    @Override
    public boolean open(int txId, long fileCookie, int fileOffset, WarmupElementWriteMetadata warmupElementWriteMetadata, List<DictionaryWarmInfo> outDictionaryWarmInfos)
    {
        // now create the native tx
        try {
            storageWriterContext = storageWriterService.open(txId, fileCookie, fileOffset, storageWriterSplitConfiguration, warmupElementWriteMetadata, outDictionaryWarmInfos);
            if (storageWriterContext == null) {
                return false;
            }
            writerOpened = true;
        }
        catch (Exception e) {
            logger.error("we create failed %s", e.getMessage());
            throw e;
        }
        return true;
    }

    @Override
    public boolean appendPage(Page page, int totalRecords)
    {
        try {
            if ((long) totalRecords + (long) page.getPositionCount() >= Integer.MAX_VALUE) {
                throw new MaxRowsException();
            }
            return storageWriterService.appendPage(page, storageWriterContext);
        }
        catch (TrinoException te) {
            abort(ExceptionThrower.isNativeException(te));
            return false;
        }
        catch (MaxRowsException maxRowsException) {
            abort(false);
            abortedWarmupElement = WarmUpElement.builder(abortedWarmupElement).state(new WarmUpElementState(maxRowsException.getState())).build();
            return false;
        }
        catch (Exception e) { // in case of exception the writer has aborted the tx internally already, we need to release it now
            abort(false);
            return false;
        }
    }

    @Override
    public WarmSinkResult close(int totalRecords)
    {
        try {
            if (!writerOpened) {
                return new WarmSinkResult(abortedWarmupElement, 0);
            }
            return storageWriterService.close(totalRecords, storageWriterSplitConfiguration, storageWriterContext);
        }
        finally {
            writerOpened = false;
        }
    }

    @Override
    public void abort(boolean nativeThrowed)
    {
        try {
            if (writerOpened) {
                abortedWarmupElement = storageWriterService.abort(nativeThrowed, storageWriterContext, storageWriterSplitConfiguration);
            }
        }
        finally {
            writerOpened = false;
        }
    }
}
