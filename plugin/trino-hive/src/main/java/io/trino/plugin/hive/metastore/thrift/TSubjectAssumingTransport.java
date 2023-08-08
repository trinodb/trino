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

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.auth.Subject;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport
public class TSubjectAssumingTransport
        extends TFilterTransport
{
    private final Subject subject;

    public TSubjectAssumingTransport(TTransport transport, Subject subject)
    {
        super(transport);
        this.subject = requireNonNull(subject, "ugi is null");
    }

    @Override
    public void open()
            throws TTransportException
    {
        try {
            Subject.doAs(subject, (PrivilegedExceptionAction<?>) () -> {
                transport.open();
                return null;
            });
        }
        catch (PrivilegedActionException e) {
            throwIfInstanceOf(e.getCause(), TTransportException.class);
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }
}
