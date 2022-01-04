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
package io.trino.sql.planner;

import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.PlannerContext;

import java.util.Optional;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public final class TestingPlannerContext
{
    public static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder().build();

    private TestingPlannerContext() {}

    public static Builder plannerContextBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<Metadata> metadata = Optional.empty();

        private Builder() {}

        public Builder withMetadata(Metadata metadata)
        {
            this.metadata = Optional.of(metadata);
            return this;
        }

        public PlannerContext build()
        {
            return new PlannerContext(
                    metadata.orElseGet(MetadataManager::createTestMetadataManager),
                    new TypeOperators(),
                    new InternalBlockEncodingSerde(new BlockEncodingManager(), TESTING_TYPE_MANAGER),
                    TESTING_TYPE_MANAGER);
        }
    }
}
