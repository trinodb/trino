
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
package io.trino.plugin.warp.gen.stats;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.metrics.VaradaStatType;
import io.trino.plugin.varada.metrics.VaradaStatsBase;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.ANY)
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName"})
public final class VaradaStatsPushdownPredicates
        extends VaradaStatsBase
{
    /* This class file is auto-generated from pushdownPredicates xml file for statistics and counters */
    private final String group;

    private final LongAdder failed_rewrite_expression = new LongAdder();
    private final LongAdder failed_rewrite_to_native_expression = new LongAdder();
    private final LongAdder unsupported_or_functions = new LongAdder();
    private final LongAdder unsupported_functions = new LongAdder();
    private final LongAdder unsupported_functions_composite = new LongAdder();
    private final LongAdder unsupported_functions_native = new LongAdder();
    private final LongAdder unsupported_expression_depth = new LongAdder();

    @JsonCreator
    public VaradaStatsPushdownPredicates(@JsonProperty("group") String group)
    {
        super(createKey(group), VaradaStatType.Worker);

        this.group = group;
    }

    @JsonProperty
    @Managed
    public String getGroup()
    {
        return group;
    }

    @JsonIgnore
    @Managed
    public long getfailed_rewrite_expression()
    {
        return failed_rewrite_expression.longValue();
    }

    public void incfailed_rewrite_expression()
    {
        failed_rewrite_expression.increment();
    }

    public void addfailed_rewrite_expression(long val)
    {
        failed_rewrite_expression.add(val);
    }

    public void setfailed_rewrite_expression(long val)
    {
        failed_rewrite_expression.reset();
        addfailed_rewrite_expression(val);
    }

    @JsonIgnore
    @Managed
    public long getfailed_rewrite_to_native_expression()
    {
        return failed_rewrite_to_native_expression.longValue();
    }

    public void incfailed_rewrite_to_native_expression()
    {
        failed_rewrite_to_native_expression.increment();
    }

    public void addfailed_rewrite_to_native_expression(long val)
    {
        failed_rewrite_to_native_expression.add(val);
    }

    public void setfailed_rewrite_to_native_expression(long val)
    {
        failed_rewrite_to_native_expression.reset();
        addfailed_rewrite_to_native_expression(val);
    }

    @JsonIgnore
    @Managed
    public long getunsupported_or_functions()
    {
        return unsupported_or_functions.longValue();
    }

    public void incunsupported_or_functions()
    {
        unsupported_or_functions.increment();
    }

    public void addunsupported_or_functions(long val)
    {
        unsupported_or_functions.add(val);
    }

    public void setunsupported_or_functions(long val)
    {
        unsupported_or_functions.reset();
        addunsupported_or_functions(val);
    }

    @JsonIgnore
    @Managed
    public long getunsupported_functions()
    {
        return unsupported_functions.longValue();
    }

    public void incunsupported_functions()
    {
        unsupported_functions.increment();
    }

    public void addunsupported_functions(long val)
    {
        unsupported_functions.add(val);
    }

    public void setunsupported_functions(long val)
    {
        unsupported_functions.reset();
        addunsupported_functions(val);
    }

    @JsonIgnore
    @Managed
    public long getunsupported_functions_composite()
    {
        return unsupported_functions_composite.longValue();
    }

    public void incunsupported_functions_composite()
    {
        unsupported_functions_composite.increment();
    }

    public void addunsupported_functions_composite(long val)
    {
        unsupported_functions_composite.add(val);
    }

    public void setunsupported_functions_composite(long val)
    {
        unsupported_functions_composite.reset();
        addunsupported_functions_composite(val);
    }

    @JsonIgnore
    @Managed
    public long getunsupported_functions_native()
    {
        return unsupported_functions_native.longValue();
    }

    public void incunsupported_functions_native()
    {
        unsupported_functions_native.increment();
    }

    public void addunsupported_functions_native(long val)
    {
        unsupported_functions_native.add(val);
    }

    public void setunsupported_functions_native(long val)
    {
        unsupported_functions_native.reset();
        addunsupported_functions_native(val);
    }

    @JsonIgnore
    @Managed
    public long getunsupported_expression_depth()
    {
        return unsupported_expression_depth.longValue();
    }

    public void incunsupported_expression_depth()
    {
        unsupported_expression_depth.increment();
    }

    public void addunsupported_expression_depth(long val)
    {
        unsupported_expression_depth.add(val);
    }

    public void setunsupported_expression_depth(long val)
    {
        unsupported_expression_depth.reset();
        addunsupported_expression_depth(val);
    }

    public static VaradaStatsPushdownPredicates create(String group)
    {
        return new VaradaStatsPushdownPredicates(group);
    }

    public static String createKey(String group)
    {
        return new StringJoiner(".").add(group).toString();
    }

    @Override
    public void merge(VaradaStatsBase varadaStatsBase)
    {
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("failed_rewrite_expression", failed_rewrite_expression);
        ret.put("failed_rewrite_to_native_expression", failed_rewrite_to_native_expression);
        ret.put("unsupported_or_functions", unsupported_or_functions);
        ret.put("unsupported_functions", unsupported_functions);
        ret.put("unsupported_functions_composite", unsupported_functions_composite);
        ret.put("unsupported_functions_native", unsupported_functions_native);
        ret.put("unsupported_expression_depth", unsupported_expression_depth);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsPushdownPredicates other = (VaradaStatsPushdownPredicates) varadaStatsBase;
        this.failed_rewrite_expression.add(other.failed_rewrite_expression.longValue());
        this.failed_rewrite_to_native_expression.add(other.failed_rewrite_to_native_expression.longValue());
        this.unsupported_or_functions.add(other.unsupported_or_functions.longValue());
        this.unsupported_functions.add(other.unsupported_functions.longValue());
        this.unsupported_functions_composite.add(other.unsupported_functions_composite.longValue());
        this.unsupported_functions_native.add(other.unsupported_functions_native.longValue());
        this.unsupported_expression_depth.add(other.unsupported_expression_depth.longValue());
    }

    @Override
    public void reset()
    {
        failed_rewrite_expression.reset();
        failed_rewrite_to_native_expression.reset();
        unsupported_or_functions.reset();
        unsupported_functions.reset();
        unsupported_functions_composite.reset();
        unsupported_functions_native.reset();
        unsupported_expression_depth.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":failed_rewrite_expression", failed_rewrite_expression.longValue());
        res.put(getJmxKey() + ":failed_rewrite_to_native_expression", failed_rewrite_to_native_expression.longValue());
        res.put(getJmxKey() + ":unsupported_or_functions", unsupported_or_functions.longValue());
        res.put(getJmxKey() + ":unsupported_functions", unsupported_functions.longValue());
        res.put(getJmxKey() + ":unsupported_functions_composite", unsupported_functions_composite.longValue());
        res.put(getJmxKey() + ":unsupported_functions_native", unsupported_functions_native.longValue());
        res.put(getJmxKey() + ":unsupported_expression_depth", unsupported_expression_depth.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("failed_rewrite_expression", getfailed_rewrite_expression());
        res.put("failed_rewrite_to_native_expression", getfailed_rewrite_to_native_expression());
        res.put("unsupported_or_functions", getunsupported_or_functions());
        res.put("unsupported_functions", getunsupported_functions());
        res.put("unsupported_functions_composite", getunsupported_functions_composite());
        res.put("unsupported_functions_native", getunsupported_functions_native());
        res.put("unsupported_expression_depth", getunsupported_expression_depth());
        return res;
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }

    public int getNumberOfMetrics()
    {
        return 7;
    }
}
