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

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public abstract class TFilterTransport
        extends TTransport
{
    protected final TTransport transport;

    protected TFilterTransport(TTransport transport)
    {
        this.transport = requireNonNull(transport, "transport is null");
    }

    @Override
    public boolean isOpen()
    {
        return transport.isOpen();
    }

    @Override
    public boolean peek()
    {
        return transport.peek();
    }

    @Override
    public void open()
            throws TTransportException
    {
        transport.open();
    }

    @Override
    public void close()
    {
        transport.close();
    }

    @Override
    public int read(ByteBuffer dst)
            throws TTransportException
    {
        return transport.read(dst);
    }

    @Override
    public int read(byte[] buf, int off, int len)
            throws TTransportException
    {
        return transport.read(buf, off, len);
    }

    @Override
    public int readAll(byte[] buf, int off, int len)
            throws TTransportException
    {
        return transport.readAll(buf, off, len);
    }

    @Override
    public void write(byte[] buf)
            throws TTransportException
    {
        transport.write(buf);
    }

    @Override
    public void write(byte[] buf, int off, int len)
            throws TTransportException
    {
        transport.write(buf, off, len);
    }

    @Override
    public int write(ByteBuffer src)
            throws TTransportException
    {
        return transport.write(src);
    }

    @Override
    public void flush()
            throws TTransportException
    {
        transport.flush();
    }

    @Override
    public byte[] getBuffer()
    {
        return transport.getBuffer();
    }

    @Override
    public int getBufferPosition()
    {
        return transport.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer()
    {
        return transport.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len)
    {
        transport.consumeBuffer(len);
    }

    @Override
    public TConfiguration getConfiguration()
    {
        return transport.getConfiguration();
    }

    @Override
    public void updateKnownMessageSize(long size)
            throws TTransportException
    {
        transport.updateKnownMessageSize(size);
    }

    @Override
    public void checkReadBytesAvailable(long numBytes)
            throws TTransportException
    {
        transport.checkReadBytesAvailable(numBytes);
    }
}
