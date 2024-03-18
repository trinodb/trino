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
package io.trino.plugin.varada.storage.read;

import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.varada.storage.read.fill.BlockFiller;

import java.util.List;

public record StorageCollectorArgs(List<BlockFiller<?>> blockFillers,
                                   int numChunksInRange,
                                   int fixedLengthStringLimit,
                                   int[] weCollectParams,
                                   long[][] collectBuffIds,
                                   List<ReadJuffersWarmUpElement> collectJuffersWE,
                                   byte[] collectStoreBuff,
                                   byte[] storeRowListBuff,
                                   byte[] collect2MatchParams,
                                   QueryParams queryParams,
                                   int chunkSize,
                                   int numChunks,
                                   long fileCookie,
                                   ChunksQueue chunksQueue)
{
}
