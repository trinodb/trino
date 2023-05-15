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
package io.trino.plugin.raptor.legacy;

import com.google.inject.Inject;
import io.trino.plugin.raptor.legacy.metadata.ForMetadata;
import io.trino.plugin.raptor.legacy.metadata.ShardManager;
import org.jdbi.v3.core.Jdbi;

import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

public class RaptorMetadataFactory
{
    private final Jdbi dbi;
    private final ShardManager shardManager;

    @Inject
    public RaptorMetadataFactory(@ForMetadata Jdbi dbi, ShardManager shardManager)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
    }

    public RaptorMetadata create(LongConsumer beginDeleteForTableId)
    {
        return new RaptorMetadata(dbi, shardManager, beginDeleteForTableId);
    }
}
