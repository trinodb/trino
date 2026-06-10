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
package io.trino.plugin.starrocks;

import io.grpc.netty.NettyChannelBuilder;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.flight.impl.Flight;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

final class TestArrowFlightRuntimeCompatibility
{
    @Test
    void testFlightTicketClassInitializes()
    {
        assertThatCode(Flight.Ticket::getDefaultInstance)
                .doesNotThrowAnyException();
    }

    @Test
    void testGrpcNettyChannelBuilderClassInitializes()
    {
        assertThatCode(() -> NettyChannelBuilder.forAddress("localhost", 0).usePlaintext())
                .doesNotThrowAnyException();
    }

    @Test
    void testOnlyUnsupportedPartitionPlanningFallsBackToSingleSplit()
    {
        assertThat(AdbcStarRocksFlightSqlClient.shouldFallbackToSingleSplit(AdbcException.notImplemented("partitioned execution is not supported")))
                .isTrue();
        assertThat(AdbcStarRocksFlightSqlClient.shouldFallbackToSingleSplit(AdbcException.invalidState("authentication failed")))
                .isFalse();
    }
}
