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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMinimalFunctionalityWithoutKeyPrefix
        extends AbstractTestMinimalFunctionality
{
    @Override
    protected Map<String, String> connectorProperties()
    {
        return ImmutableMap.of("redis.key-prefix-schema-table", "false");
    }

    @Test
    public void testStringValueWhereClauseHasData()
    {
        assertThat(assertions.query("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')".formatted(stringValueTableName, stringValueTableName, tableName)))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query("SELECT count(1) FROM %s WHERE redis_key = '%s:999'".formatted(stringValueTableName, tableName)))
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')".formatted(stringValueTableName, tableName, tableName)))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    public void testHashValueWhereClauseHasData()
    {
        assertThat(assertions.query("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')".formatted(hashValueTableName, hashValueTableName, hashValueTableName)))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query("SELECT count(1) FROM %s WHERE redis_key = '%s:999'".formatted(hashValueTableName, hashValueTableName)))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testHashValueWhereClauseHasNoData()
    {
        assertThat(assertions.query("SELECT count(1) FROM %s WHERE redis_key = 'does_not_exist'".formatted(hashValueTableName)))
                .matches("VALUES BIGINT '0'");

        assertThat(assertions.query("SELECT count(1) FROM %s WHERE redis_key = '%s:999' AND id = 1".formatted(hashValueTableName, hashValueTableName)))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testHashValueWhereClauseError()
    {
        assertThatThrownBy(() -> queryRunner.execute(
                "SELECT count(1) FROM %s WHERE redis_key = '%s:999'".formatted(hashValueTableName, stringValueTableName)))
                .hasMessageContaining("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
