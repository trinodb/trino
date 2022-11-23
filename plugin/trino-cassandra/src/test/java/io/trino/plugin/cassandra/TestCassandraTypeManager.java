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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.cassandra.CassandraTestingUtils.CASSANDRA_TYPE_MANAGER;
import static org.testng.Assert.assertTrue;

public class TestCassandraTypeManager
{
    @Test
    public void testJsonArrayEncoding()
    {
        assertTrue(isValidJson(CASSANDRA_TYPE_MANAGER.buildArrayValue(Lists.newArrayList("one", "two", "three\""), DataTypes.TEXT)));
        assertTrue(isValidJson(CASSANDRA_TYPE_MANAGER.buildArrayValue(Lists.newArrayList(1, 2, 3), DataTypes.INT)));
        assertTrue(isValidJson(CASSANDRA_TYPE_MANAGER.buildArrayValue(Lists.newArrayList(100000L, 200000000L, 3000000000L), DataTypes.BIGINT)));
        assertTrue(isValidJson(CASSANDRA_TYPE_MANAGER.buildArrayValue(Lists.newArrayList(1.0, 2.0, 3.0), DataTypes.DOUBLE)));
        assertTrue(isValidJson(CASSANDRA_TYPE_MANAGER.buildArrayValue(Lists.newArrayList((short) -32768, (short) 0, (short) 32767), DataTypes.SMALLINT)));
        assertTrue(isValidJson(CASSANDRA_TYPE_MANAGER.buildArrayValue(Lists.newArrayList((byte) -128, (byte) 0, (byte) 127), DataTypes.TINYINT)));
        assertTrue(isValidJson(CASSANDRA_TYPE_MANAGER.buildArrayValue(Lists.newArrayList("1970-01-01", "5555-06-15", "9999-12-31"), DataTypes.DATE)));
    }

    private static void continueWhileNotNull(JsonParser parser, JsonToken token)
            throws IOException
    {
        if (token != null) {
            continueWhileNotNull(parser, parser.nextToken());
        }
    }

    private static boolean isValidJson(String json)
    {
        boolean valid = false;
        try {
            JsonParser parser = new ObjectMapper().getFactory()
                    .createParser(json);
            continueWhileNotNull(parser, parser.nextToken());
            valid = true;
        }
        catch (IOException ignored) {
        }
        return valid;
    }
}
