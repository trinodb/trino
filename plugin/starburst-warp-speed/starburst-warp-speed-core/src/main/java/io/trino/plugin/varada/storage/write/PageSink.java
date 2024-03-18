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

import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.spi.Page;

import java.util.List;

public interface PageSink
{
    boolean open(int txId, long fileCookie, int fileOffset, WarmupElementWriteMetadata warmupElementWriteMetadata, List<DictionaryWarmInfo> outDictionaryWarmInfos);

    boolean appendPage(Page page, int totalRecords);

    WarmSinkResult close(int totalRecords);

    void abort(boolean nativeThrowed);
}
