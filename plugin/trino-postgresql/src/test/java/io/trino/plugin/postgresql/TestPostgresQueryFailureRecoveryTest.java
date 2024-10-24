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
package io.trino.plugin.postgresql;

import io.trino.operator.RetryPolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestPostgresQueryFailureRecoveryTest
        extends BasePostgresFailureRecoveryTest
{
    public TestPostgresQueryFailureRecoveryTest()
    {
        super(RetryPolicy.QUERY);
    }

    @Test
    @Override
    protected void testDeleteWithSubquery()
    {
        // TODO: support merge with fte https://github.com/trinodb/trino/issues/23345
        assertThatThrownBy(super::testDeleteWithSubquery).hasMessageContaining("expected: OptionalLong[527]\n but was: OptionalLong[0]");
        abort("skipped");
    }

    @Test
    @Override
    protected void testMerge()
    {
        // TODO: support merge with fte https://github.com/trinodb/trino/issues/23345
        assertThatThrownBy(super::testMerge).hasMessageContaining("expected: OptionalLong[15000]\n but was: OptionalLong[14745]");
        abort("skipped");
    }
}
