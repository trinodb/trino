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
package io.trino.plugin.raptor.legacy.storage.organization;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestShardOrganizer
{
    @Test
    @Timeout(5)
    public void testShardOrganizerInProgress()
            throws Exception
    {
        CountDownLatch canComplete = new CountDownLatch(1);
        ShardOrganizer organizer = new ShardOrganizer(
                organizationSet -> () -> checkState(awaitUninterruptibly(canComplete, 10, SECONDS)),
                1);

        Set<UUID> shards = ImmutableSet.of(UUID.randomUUID());
        OrganizationSet organizationSet = new OrganizationSet(1L, shards, OptionalInt.empty());

        organizer.enqueue(organizationSet);

        assertThat(organizer.inProgress(getOnlyElement(shards))).isTrue();
        assertThat(organizer.getShardsInProgress()).isEqualTo(1);

        canComplete.countDown();
        while (organizer.inProgress(getOnlyElement(shards))) {
            MILLISECONDS.sleep(10);
        }
        assertThat(organizer.inProgress(getOnlyElement(shards))).isFalse();
        assertThat(organizer.getShardsInProgress()).isEqualTo(0);
        organizer.shutdown();
    }
}
