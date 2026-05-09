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
package io.trino.tests.product.utils;

import org.junit.jupiter.api.Test;

import static io.trino.tests.product.utils.QueryExecutors.isDatabricksTransientFailure;
import static org.assertj.core.api.Assertions.assertThat;

class TestQueryExecutors
{
    @Test
    void testDatabricksTransientFailureClassification()
    {
        assertThat(isDatabricksTransientFailure(new RuntimeException("HTTP Response code: 502"))).isTrue();
        assertThat(isDatabricksTransientFailure(new RuntimeException("The current cluster state is Pending"))).isTrue();
        assertThat(isDatabricksTransientFailure(new RuntimeException("The current cluster state is Terminated"))).isTrue();
        assertThat(isDatabricksTransientFailure(new RuntimeException("The cluster is temporarily unavailable"))).isTrue();

        assertThat(isDatabricksTransientFailure(new RuntimeException("HTTP Response code: 503"))).isFalse();
        assertThat(isDatabricksTransientFailure(new RuntimeException("closeStatement failed after HTTP Response code: 502"))).isFalse();
        assertThat(isDatabricksTransientFailure(new RuntimeException("CloseOperation failed while The current cluster state is Terminated"))).isFalse();
    }
}
