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
package io.trino.spooling.filesystem.encryption;

import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ExceptionMappingInputStream
        extends FilterInputStream
{
    public ExceptionMappingInputStream(InputStream delegate)
    {
        super(delegate);
    }

    @Override
    public int read()
            throws IOException
    {
        try {
            return super.read();
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("Segment not found or expired", e);
        }
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        try {
            return super.read(b);
        }
        catch (FileNotFoundException e) {
            throw new IOException("Segment not found or expired", e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        try {
            return super.read(b, off, len);
        }
        catch (FileNotFoundException e) {
            throw new IOException("Segment not found or expired", e);
        }
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        try {
            return super.skip(n);
        }
        catch (FileNotFoundException e) {
            throw new IOException("Segment not found or expired", e);
        }
    }

    @Override
    public int available()
            throws IOException
    {
        try {
            return super.available();
        }
        catch (FileNotFoundException e) {
            throw new IOException("Segment not found or expired", e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            super.close();
        }
        catch (FileNotFoundException e) {
            throw new IOException("Segment not found or expired", e);
        }
    }
}
