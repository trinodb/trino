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
package io.trino.plugin.varada.dispatcher.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.spi.TrinoException;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_MATCH_COLLECT_ID_ALLOCATION;

@Singleton
public class MatchCollectIdService
{
    public static final int INVALID_ID = -1;
    private static final Logger logger = Logger.get(MatchCollectIdService.class);

    private final Queue<Integer> matchCollectIdsQueue;

    @Inject
    public MatchCollectIdService(StorageEngineConstants storageEngineConstants)
    {
        int numIds = storageEngineConstants.getMatchCollectNumIds();
        matchCollectIdsQueue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < numIds; i++) {
            matchCollectIdsQueue.add(i);
        }
    }

    // return INVALID_ID if failed
    public int allocMatchCollectId()
    {
        Integer id = matchCollectIdsQueue.poll();
        if (id == null) {
            logger.debug("failed to allocate match collect id, queue size %d", matchCollectIdsQueue.size());
            return INVALID_ID;
        }
        return id;
    }

    public void freeMatchCollectId(int id)
    {
        if (id == INVALID_ID) {
            return;
        }

        if (!matchCollectIdsQueue.offer(id)) {
            logger.error("failed to release match collect id %d", id);
            throw new TrinoException(VARADA_MATCH_COLLECT_ID_ALLOCATION, String.format("failed to release match collect id %d", id));
        }
    }

    @VisibleForTesting
    public int getNumFreeIds()
    {
        return matchCollectIdsQueue.size();
    }
}
