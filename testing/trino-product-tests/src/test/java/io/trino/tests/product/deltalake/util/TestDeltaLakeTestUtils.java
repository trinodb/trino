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
package io.trino.tests.product.deltalake.util;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.isDatabricksCommunicationFailure;
import static org.assertj.core.api.Assertions.assertThat;

class TestDeltaLakeTestUtils
{
    @Test
    void testDatabricksCommunicationFailureClassification()
    {
        SQLException httpFailure = new SQLException("[Databricks][JDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: HTTP request failed by code: 502");
        SQLException clusterUnavailable = new SQLException("[Databricks][JDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: The cluster is temporarily unavailable");
        SQLException terminatedCluster = new SQLException("[Databricks][JDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: The current cluster state is Terminated");

        assertThat(isDatabricksCommunicationFailure(httpFailure)).isTrue();
        assertThat(isDatabricksCommunicationFailure(clusterUnavailable)).isTrue();
        assertThat(isDatabricksCommunicationFailure(terminatedCluster)).isTrue();

        assertThat(isDatabricksCommunicationFailure(new SQLException("The current cluster state is Running"))).isFalse();
        assertThat(isDatabricksCommunicationFailure(new SQLException("syntax error"))).isFalse();
        assertThat(isDatabricksCommunicationFailure(new RuntimeException("HTTP request failed by code: 502"))).isFalse();
    }
}
