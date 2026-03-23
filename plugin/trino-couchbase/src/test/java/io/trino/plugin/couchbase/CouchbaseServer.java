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
package io.trino.plugin.couchbase;

import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.utility.DockerImageName;

public class CouchbaseServer
        implements AutoCloseable
{
    private CouchbaseContainer container;

    public CouchbaseServer(String bucketName)
    {
        DockerImageName cbImage = DockerImageName.parse("couchbase:enterprise-8.0.0").asCompatibleSubstituteFor("couchbase/server");
        this.container = new CouchbaseContainer(cbImage).withBucket(new BucketDefinition(bucketName)).withEnabledServices(CouchbaseService.KV, CouchbaseService.INDEX, CouchbaseService.QUERY, CouchbaseService.SEARCH).withStartupAttempts(3);
        this.container.start();
    }

    public String getConnectionString()
    {
        return container.getConnectionString();
    }

    public String getUsername()
    {
        return container.getUsername();
    }

    public String getPassword()
    {
        return container.getPassword();
    }

    public void close()
    {
        container.close();
    }
}
