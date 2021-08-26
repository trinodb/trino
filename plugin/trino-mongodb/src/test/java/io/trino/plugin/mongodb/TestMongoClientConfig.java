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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoCredential;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestMongoClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MongoClientConfig.class)
                .setSchemaCollection("_schema")
                .setCaseInsensitiveNameMatching(false)
                .setSeeds("")
                .setCredentials("")
                .setMinConnectionsPerHost(0)
                .setConnectionsPerHost(100)
                .setMaxWaitTime(120_000)
                .setConnectionTimeout(10_000)
                .setSocketTimeout(0)
                .setSocketKeepAlive(true)
                .setSslEnabled(false)
                .setMaxConnectionIdleTime(0)
                .setCursorBatchSize(0)
                .setReadPreference(ReadPreferenceType.PRIMARY)
                .setWriteConcern(WriteConcernType.ACKNOWLEDGED)
                .setRequiredReplicaSetName(null)
                .setImplicitRowFieldPrefix("_pos"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("mongodb.schema-collection", "_my_schema")
                .put("mongodb.case-insensitive-name-matching", "true")
                .put("mongodb.seeds", "host1,host2:27016")
                .put("mongodb.credentials", "username:password@collection")
                .put("mongodb.min-connections-per-host", "1")
                .put("mongodb.connections-per-host", "99")
                .put("mongodb.max-wait-time", "120001")
                .put("mongodb.connection-timeout", "9999")
                .put("mongodb.socket-timeout", "1")
                .put("mongodb.socket-keep-alive", "false")
                .put("mongodb.ssl.enabled", "true")
                .put("mongodb.max-connection-idle-time", "180000")
                .put("mongodb.cursor-batch-size", "1")
                .put("mongodb.read-preference", "NEAREST")
                .put("mongodb.write-concern", "UNACKNOWLEDGED")
                .put("mongodb.required-replica-set", "replica_set")
                .put("mongodb.implicit-row-field-prefix", "_prefix")
                .build();

        MongoClientConfig expected = new MongoClientConfig()
                .setSchemaCollection("_my_schema")
                .setCaseInsensitiveNameMatching(true)
                .setSeeds("host1", "host2:27016")
                .setCredentials("username:password@collection")
                .setMinConnectionsPerHost(1)
                .setConnectionsPerHost(99)
                .setMaxWaitTime(120_001)
                .setConnectionTimeout(9_999)
                .setSocketTimeout(1)
                .setSocketKeepAlive(false)
                .setSslEnabled(true)
                .setMaxConnectionIdleTime(180_000)
                .setCursorBatchSize(1)
                .setReadPreference(ReadPreferenceType.NEAREST)
                .setWriteConcern(WriteConcernType.UNACKNOWLEDGED)
                .setRequiredReplicaSetName("replica_set")
                .setImplicitRowFieldPrefix("_prefix");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testSpecialCharacterCredential()
    {
        MongoClientConfig config = new MongoClientConfig()
                .setCredentials("username:P@ss:w0rd@database");

        MongoCredential credential = config.getCredentials().get(0);
        MongoCredential expected = MongoCredential.createCredential("username", "database", "P@ss:w0rd".toCharArray());
        assertEquals(credential, expected);
    }
}
