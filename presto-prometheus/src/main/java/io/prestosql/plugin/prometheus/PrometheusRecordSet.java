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
package io.prestosql.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.List;

import static io.prestosql.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_UNKNOWN_ERROR;
import static java.util.Objects.requireNonNull;

public class PrometheusRecordSet
        implements RecordSet
{
    private final List<PrometheusColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private ByteSource byteSource;

    public PrometheusRecordSet(PrometheusSplit split, List<PrometheusColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (PrometheusColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        try {
            byteSource = ByteSource.wrap(PrometheusClient.getHttpResponse(split.getUri()).bytes());
        }
        catch (MalformedURLException e) {
            throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "split URL to use with Prometheus has an error: " + e.getMessage());
        }
        catch (IOException e) {
            throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "error with Prometheus response: " + e.getMessage());
        }
        catch (URISyntaxException e) {
            throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "error with Prometheus response: " + e.getMessage());
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new PrometheusRecordCursor(columnHandles, byteSource);
    }
}
