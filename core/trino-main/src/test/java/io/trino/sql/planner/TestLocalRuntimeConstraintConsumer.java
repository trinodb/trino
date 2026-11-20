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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.operator.RuntimeConstraintSourceConsumer.Observation;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLocalRuntimeConstraintConsumer
{
    @Test
    public void testAggregatesPartitionsAndObservationsByLane()
    {
        AtomicReference<RuntimeMembershipPayload> payload = new AtomicReference<>();
        LocalRuntimeConstraintConsumer consumer = new LocalRuntimeConstraintConsumer(
                ImmutableList.of(3, 7),
                ImmutableList.of(INTEGER, INTEGER),
                payload::set,
                DataSize.of(100, KILOBYTE));
        consumer.setPartitionCount(2);

        consumer.addPartition(
                ImmutableList.of(
                        Domain.singleValue(INTEGER, 10L),
                        Domain.singleValue(INTEGER, 15L)),
                new Observation(true, ImmutableList.of(false, true)));
        assertThat(payload).hasValue(null);

        consumer.addPartition(
                ImmutableList.of(
                        Domain.singleValue(INTEGER, 20L),
                        Domain.singleValue(INTEGER, 30L)),
                new Observation(true, ImmutableList.of(true, false)));

        assertThat(payload).hasValue(new RuntimeMembershipPayload(
                ImmutableList.of(
                        new Lane(Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L)), true),
                        new Lane(Domain.multipleValues(INTEGER, ImmutableList.of(15L, 30L)), true)),
                ORDINARY,
                true));
    }

    @Test
    public void testNoneLaneDoesNotDiscardSiblingLane()
    {
        AtomicReference<RuntimeMembershipPayload> payload = new AtomicReference<>();
        LocalRuntimeConstraintConsumer consumer = new LocalRuntimeConstraintConsumer(
                ImmutableList.of(0, 1),
                ImmutableList.of(INTEGER, INTEGER),
                payload::set,
                DataSize.of(100, KILOBYTE));
        consumer.setPartitionCount(1);

        consumer.addPartition(
                ImmutableList.of(Domain.none(INTEGER), Domain.singleValue(INTEGER, 7L)),
                new Observation(true, ImmutableList.of(true, false)));

        assertThat(payload).hasValue(new RuntimeMembershipPayload(
                ImmutableList.of(new Lane(Domain.none(INTEGER), true), new Lane(Domain.singleValue(INTEGER, 7L), false)),
                ORDINARY,
                true));
    }

    @Test
    public void testNoOperatorsPublishesEmptyBuild()
    {
        AtomicReference<RuntimeMembershipPayload> payload = new AtomicReference<>();
        LocalRuntimeConstraintConsumer consumer = new LocalRuntimeConstraintConsumer(
                ImmutableList.of(0),
                ImmutableList.of(INTEGER),
                payload::set,
                DataSize.of(100, KILOBYTE));

        consumer.setPartitionCount(0);

        assertThat(payload).hasValue(new RuntimeMembershipPayload(ImmutableList.of(Domain.none(INTEGER)), ORDINARY));
    }
}
