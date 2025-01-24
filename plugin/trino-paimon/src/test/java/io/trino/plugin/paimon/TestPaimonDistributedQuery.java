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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.QueryRunner;

/**
 * The test of TrinoDistributedQuery.
 */
final class TestPaimonDistributedQuery
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PaimonQueryRunner.createPrestoQueryRunner(ImmutableMap.of());
    }

    @Override
    public void testImplicitCastToRowWithFieldsRequiringDelimitation()
    {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testInsertWithCoercion()
    {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testResetSession()
    {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testSetSession()
    {
        throw new RuntimeException("TODO: test not implemented yet");
    }

    @Override
    public void testShowSession()
    {
        throw new RuntimeException("TODO: test not implemented yet");
    }
}
