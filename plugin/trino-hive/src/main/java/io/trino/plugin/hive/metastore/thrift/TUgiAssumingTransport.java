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
package io.trino.plugin.hive.metastore.thrift;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import static io.trino.hdfs.authentication.UserGroupInformationUtils.executeActionInDoAs;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport
public class TUgiAssumingTransport
        extends TFilterTransport
{
    private final UserGroupInformation ugi;

    public TUgiAssumingTransport(TTransport transport, UserGroupInformation ugi)
    {
        super(transport);
        this.ugi = requireNonNull(ugi, "ugi is null");
    }

    @Override
    public void open()
            throws TTransportException
    {
        executeActionInDoAs(ugi, () -> {
            transport.open();
            return null;
        });
    }
}
