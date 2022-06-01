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
package io.trino.plugin.kudu;

import org.apache.kudu.Schema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.AlterTableResponse;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.DeleteTableResponse;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.ListTablesResponse;

import java.io.IOException;

public abstract class ForwardingKuduClient
        implements KuduClientWrapper
{
    protected abstract KuduClient delegate();

    @Override
    public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
            throws KuduException
    {
        return delegate().createTable(name, schema, builder);
    }

    @Override
    public DeleteTableResponse deleteTable(String name)
            throws KuduException
    {
        return delegate().deleteTable(name);
    }

    @Override
    public AlterTableResponse alterTable(String name, AlterTableOptions ato)
            throws KuduException
    {
        return delegate().alterTable(name, ato);
    }

    @Override
    public ListTablesResponse getTablesList()
            throws KuduException
    {
        return delegate().getTablesList();
    }

    @Override
    public ListTablesResponse getTablesList(String nameFilter)
            throws KuduException
    {
        return delegate().getTablesList(nameFilter);
    }

    @Override
    public boolean tableExists(String name)
            throws KuduException
    {
        return delegate().tableExists(name);
    }

    @Override
    public KuduTable openTable(final String name)
            throws KuduException
    {
        return delegate().openTable(name);
    }

    @Override
    public KuduScanner.KuduScannerBuilder newScannerBuilder(KuduTable table)
    {
        return delegate().newScannerBuilder(table);
    }

    @Override
    public KuduScanToken.KuduScanTokenBuilder newScanTokenBuilder(KuduTable table)
    {
        return delegate().newScanTokenBuilder(table);
    }

    @Override
    public KuduSession newSession()
    {
        return delegate().newSession();
    }

    @Override
    public KuduScanner deserializeIntoScanner(byte[] serializedScanToken)
            throws IOException
    {
        return KuduScanToken.deserializeIntoScanner(serializedScanToken, delegate());
    }

    @Override
    public void close()
            throws KuduException
    {
        delegate().close();
    }
}
