<?xml version="1.0" encoding="UTF-8"?>
<!--

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
    <xsl:variable name="upper" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ'" />
    <xsl:variable name="lower" select="'abcdefghijklmnopqrstuvwxyz'" />

    <xsl:template name="License">
        <xsl:text>&#10;</xsl:text>
        <xsl:text>/*&#10;</xsl:text>
        <xsl:text> * Licensed under the Apache License, Version 2.0 (the "License");&#10;</xsl:text>
        <xsl:text> * you may not use this file except in compliance with the License.&#10;</xsl:text>
        <xsl:text> * You may obtain a copy of the License at&#10;</xsl:text>
        <xsl:text> *&#10;</xsl:text>
        <xsl:text> *     http://www.apache.org/licenses/LICENSE-2.0&#10;</xsl:text>
        <xsl:text> *&#10;</xsl:text>
        <xsl:text> * Unless required by applicable law or agreed to in writing, software&#10;</xsl:text>
        <xsl:text> * distributed under the License is distributed on an "AS IS" BASIS,&#10;</xsl:text>
        <xsl:text> * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.&#10;</xsl:text>
        <xsl:text> * See the License for the specific language governing permissions and&#10;</xsl:text>
        <xsl:text> * limitations under the License.&#10;</xsl:text>
        <xsl:text> */&#10;</xsl:text>
    </xsl:template>
    <xsl:template name="Capitalize">
        <xsl:param name="s" />
        <xsl:value-of select="translate($s, $lower, $upper)" />
    </xsl:template>
    <xsl:template name="NewLine">
        <xsl:text>&#10;</xsl:text>
    </xsl:template>
    <xsl:template name="GenerateMetricClassName">
        <xsl:param name="fileName" />
        <xsl:text>VaradaStats</xsl:text>
        <xsl:call-template name="Capitalize">
            <xsl:with-param name="s">
                <xsl:value-of select="substring($fileName, 1, 1)" />
            </xsl:with-param>
        </xsl:call-template>
        <xsl:value-of select="substring($fileName, 2)" />
    </xsl:template>
    <xsl:template name="appendMethodParam">
        <xsl:param name="type" />
        <xsl:param name="paramName" />
        <xsl:param name="pos" />

        <xsl:if test="$pos != 1">, </xsl:if>
        <xsl:call-template name="declareParam">
            <xsl:with-param name="type">
                <xsl:value-of select="$type" />
            </xsl:with-param>
            <xsl:with-param name="paramName">
                <xsl:value-of select="$paramName" />
            </xsl:with-param>
        </xsl:call-template>
    </xsl:template>
    <xsl:template name="declareParam">
        <xsl:param name="type" />
        <xsl:param name="paramName" />
        <xsl:value-of select="$type" />
        <xsl:text> </xsl:text>
        <xsl:value-of select="$paramName" />
    </xsl:template>
    <xsl:template name="appendJoinedParam">
        <xsl:param name="type" />
        <xsl:param name="paramName" />

        <xsl:text>.add(</xsl:text>
        <xsl:choose>
            <xsl:when test="$type = 'String'">
                <xsl:value-of select="$paramName" />
            </xsl:when>
            <xsl:otherwise>
                <xsl:text>String.valueOf(</xsl:text>
                <xsl:value-of select="$paramName" />
                <xsl:text>)</xsl:text>
            </xsl:otherwise>
        </xsl:choose>
        <xsl:text>)</xsl:text>
    </xsl:template>
    <xsl:template name="AddKeyFields">
        <xsl:for-each select="Key">
            <xsl:text>    private final </xsl:text>
            <xsl:call-template name="declareParam">
                <xsl:with-param name="type">
                    <xsl:value-of select="@Type" />
                </xsl:with-param>
                <xsl:with-param name="paramName">
                    <xsl:value-of select="@Name" />
                </xsl:with-param>
            </xsl:call-template>
            <xsl:text>;</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:for-each>
        <xsl:call-template name="NewLine" />
    </xsl:template>
    <xsl:template name="OutputKeysManaged">
        <xsl:text>    @JsonProperty</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    @Managed&#10;    public </xsl:text>
        <xsl:value-of select="@Type" />
        <xsl:text> get</xsl:text>
        <xsl:call-template name="Capitalize">
            <xsl:with-param name="s">
                <xsl:value-of select="substring(@Name, 1, 1)" />
            </xsl:with-param>
        </xsl:call-template>
        <xsl:value-of select="substring(@Name, 2)" />
        <xsl:text>()&#10;    {&#10;        return </xsl:text>
        <xsl:value-of select="@Name" />
        <xsl:text>;</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    }</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:call-template name="NewLine" />
    </xsl:template>
    <xsl:template name="createHasPersistentMethod">
        <xsl:if test="count(//Counter[@persistent = 'true']) > 0">
            <xsl:call-template name="NewLine" />
            <xsl:text>    @Override</xsl:text>
            <xsl:call-template name="NewLine" />
            <xsl:text>    public boolean hasPersistentMetric()</xsl:text>
            <xsl:call-template name="NewLine" />
            <xsl:text>    {</xsl:text>
            <xsl:call-template name="NewLine" />
            <xsl:text>        return true;</xsl:text>
            <xsl:call-template name="NewLine" />
            <xsl:text>    }</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:if>
    </xsl:template>
    <xsl:template name="createConstructorParams">
        <xsl:choose>
            <xsl:when test="(@Type='long' or @Type='double')">
                <xsl:text>@JsonProperty("</xsl:text><xsl:value-of select="@Name" /><xsl:text>") </xsl:text>
                <xsl:value-of select="@Type" /><xsl:text> </xsl:text>
                <xsl:value-of select="@Name" />
                <xsl:if test="position() != count(//Counter)">, </xsl:if>
            </xsl:when>
            <xsl:when test=" (@Type='min' or @Type='max')">
                <xsl:text>@JsonProperty("</xsl:text>
                <xsl:value-of select="@Type" /><xsl:text>_</xsl:text><xsl:value-of select="@Name" />
                <xsl:text>") long </xsl:text>
                <xsl:value-of select="@Type" /><xsl:text>_</xsl:text><xsl:value-of select="@Name" />
                <xsl:if test="position() != count(//Counter)">, </xsl:if>
            </xsl:when>
        </xsl:choose>
    </xsl:template>
    <xsl:template name="JsonIgnoreCreator">
        <xsl:choose>
            <xsl:when test="not (@persistent) or attribute::persistent = 'false'">
                <xsl:text>    @JsonIgnore</xsl:text>
                <xsl:call-template name="NewLine" />
            </xsl:when>
        </xsl:choose>
    </xsl:template>
    <xsl:template name="JsonPropertyCreator">
        <xsl:text>    @JsonProperty("</xsl:text><xsl:value-of select="@Name" />
        <xsl:text>")&#10;</xsl:text>
        <xsl:text>    @Managed&#10;</xsl:text>
    </xsl:template>
    <xsl:template name="createNumOfMetricsMethod">
        <xsl:param name="num_counters" />
        <xsl:call-template name="NewLine" />
        <xsl:text>    public int getNumberOfMetrics()</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    {</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>        return </xsl:text><xsl:value-of select="$num_counters" /><xsl:text>;</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    }</xsl:text>
        <xsl:call-template name="NewLine" />
    </xsl:template>
    <xsl:template name="createMergeStatsMethod">
        <xsl:param name="suffix" />
        <xsl:text>    @Override</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    public void mergeStats(VaradaStatsBase varadaStatsBase)</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    {</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>        if (varadaStatsBase == null) {</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>            return;</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>        }</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>        </xsl:text>
        <xsl:call-template name="GenerateMetricClassName">
            <xsl:with-param name="fileName">
                <xsl:value-of select="@Name" />
            </xsl:with-param>
        </xsl:call-template>
        <xsl:text> other = (</xsl:text>
        <xsl:call-template name="GenerateMetricClassName">
            <xsl:with-param name="fileName">
                <xsl:value-of select="@Name" />
            </xsl:with-param>
        </xsl:call-template>
        <xsl:text>) varadaStatsBase;</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:for-each select="Counter">
            <xsl:text>        this.</xsl:text><xsl:value-of select="@Name" /><xsl:value-of select="$suffix" /><xsl:text>.add(</xsl:text>
            <xsl:text>other.</xsl:text><xsl:value-of select="@Name" /><xsl:value-of select="$suffix" /><xsl:text>.longValue());</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:for-each>
        <xsl:for-each select="Average">
            <xsl:text>        this.</xsl:text><xsl:value-of select="@Name" /><xsl:value-of select="$suffix" /><xsl:text>.add(</xsl:text>
            <xsl:text>other.</xsl:text><xsl:value-of select="@Name" /><xsl:value-of select="$suffix" /><xsl:text>.longValue());</xsl:text>
            <xsl:call-template name="NewLine" />
            <xsl:text>        this.</xsl:text><xsl:value-of select="@Name" /><xsl:value-of select="$suffix" /><xsl:text>_Count.add(</xsl:text>
            <xsl:text>other.</xsl:text><xsl:value-of select="@Name" /><xsl:value-of select="$suffix" /><xsl:text>_Count.longValue());</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:for-each>
        <xsl:text>    }</xsl:text>
        <xsl:call-template name="NewLine" />
    </xsl:template>
    <xsl:template name="statsCounterMapperMethod">
        <xsl:call-template name="NewLine" />
        <xsl:text>    @Override</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    public </xsl:text>
        <xsl:text>Map&lt;String, Object&gt; statsCounterMapper()</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    {</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>        Map&lt;String, Object&gt; res = new HashMap&lt;&gt;();</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:for-each select="Counter">
            <xsl:if test="not(@CustomMetrics = 'False')">
                <xsl:call-template name="addFieldToMap">
                    <xsl:with-param name="fieldName">
                        <xsl:value-of select="@Name" />
                    </xsl:with-param>
                </xsl:call-template>
            </xsl:if>
        </xsl:for-each>
        <xsl:for-each select="Average">
            <xsl:if test="not(@CustomMetrics = 'False')">
                <xsl:call-template name="addFieldToMap">
                    <xsl:with-param name="fieldName">
                        <xsl:value-of select="@Name" />
                    </xsl:with-param>
                </xsl:call-template>
                <xsl:call-template name="addFieldToMap">
                    <xsl:with-param name="fieldName">
                        <xsl:value-of select="@Name" /><xsl:text>_Count</xsl:text>
                    </xsl:with-param>
                </xsl:call-template>
            </xsl:if>
        </xsl:for-each>
        <xsl:text>        return res;</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    }&#10;</xsl:text>
    </xsl:template>
    <xsl:template name="getCountersMethod">
        <xsl:call-template name="NewLine" />
        <xsl:text>    @Override</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    public </xsl:text>
        <xsl:text>Map&lt;String, LongAdder&gt; getCounters()</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    {</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>        Map&lt;String, LongAdder&gt; ret = new HashMap&lt;&gt;();</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:for-each select="Counter">
            <xsl:text>        ret.put("</xsl:text>
            <xsl:value-of select="@Name" />
            <xsl:text>", </xsl:text>
            <xsl:value-of select="@Name" />
            <xsl:text>);</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:for-each>
        <xsl:call-template name="NewLine" />
        <xsl:text>        return ret;</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    }&#10;</xsl:text>
        <xsl:call-template name="NewLine" />
    </xsl:template>
    <xsl:template name="statsLoggerMapperMethod">
        <xsl:call-template name="deltaPrintFieldsMethod" />
        <xsl:call-template name="statePrintFieldsMethod" />
    </xsl:template>
    <xsl:template name="deltaPrintFieldsMethod">
        <xsl:call-template name="NewLine" />
        <xsl:text>    @Override</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    protected </xsl:text>
        <xsl:text>Map&lt;String, Object&gt; deltaPrintFields()</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    {</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:variable name="numDeltas">
            <xsl:value-of select="1"/>
            <xsl:for-each select="Counter">
                <xsl:if test="@Print = 'delta'">
                    <xsl:value-of select="1"/>
                </xsl:if>
            </xsl:for-each>
            <xsl:for-each select="Average">
                <xsl:if test="@Print = 'state'">
                    <xsl:value-of select="1"/>
                </xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="$numDeltas > 1">
            <xsl:text>        Map&lt;String, Object&gt; res = new HashMap&lt;&gt;();</xsl:text>
            <xsl:call-template name="NewLine" />
            <xsl:for-each select="Counter">
                <xsl:if test="@Print = 'delta'">
                    <xsl:call-template name="addFieldGetterToMap">
                        <xsl:with-param name="fieldName">
                            <xsl:value-of select="@Name" />
                        </xsl:with-param>
                    </xsl:call-template>
                </xsl:if>
            </xsl:for-each>
            <xsl:for-each select="Average">
                <xsl:if test="@Print = 'delta'">
                    <xsl:call-template name="addFieldGetterToMap">
                        <xsl:with-param name="fieldName">
                            <xsl:value-of select="@Name" />
                        </xsl:with-param>
                    </xsl:call-template>
                    <xsl:call-template name="addFieldGetterToMap">
                        <xsl:with-param name="fieldName">
                            <xsl:value-of select="@Name" /><xsl:text>_Count</xsl:text>
                        </xsl:with-param>
                    </xsl:call-template>
                </xsl:if>
            </xsl:for-each>
            <xsl:text>        return res;</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:if>
        <xsl:if test="$numDeltas = 1">
            <xsl:text>        return new HashMap&lt;&gt;();</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:if>
        <xsl:text>    }&#10;</xsl:text>
    </xsl:template>
    <xsl:template name="statePrintFieldsMethod">
        <xsl:call-template name="NewLine" />
        <xsl:text>    @Override</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    protected </xsl:text>
        <xsl:text>Map&lt;String, Object&gt; statePrintFields()</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:text>    {</xsl:text>
        <xsl:call-template name="NewLine" />
        <xsl:variable name="numStates">
            <xsl:value-of select="1"/>
            <xsl:for-each select="Counter">
                <xsl:if test="@Print = 'state'">
                    <xsl:value-of select="1"/>
                </xsl:if>
            </xsl:for-each>
            <xsl:for-each select="Average">
                <xsl:if test="@Print = 'state'">
                    <xsl:value-of select="1"/>
                </xsl:if>
            </xsl:for-each>
        </xsl:variable>
        <xsl:if test="$numStates > 1">
            <xsl:text>        Map&lt;String, Object&gt; res = new HashMap&lt;&gt;();</xsl:text>
            <xsl:call-template name="NewLine" />
            <xsl:for-each select="Counter">
                <xsl:if test="@Print = 'state'">
                    <xsl:call-template name="addFieldGetterToMap">
                        <xsl:with-param name="fieldName">
                            <xsl:value-of select="@Name" />
                        </xsl:with-param>
                    </xsl:call-template>
                </xsl:if>
            </xsl:for-each>
            <xsl:for-each select="Average">
                <xsl:if test="@Print = 'state'">
                    <xsl:call-template name="addFieldGetterToMap">
                        <xsl:with-param name="fieldName">
                            <xsl:value-of select="@Name" />
                        </xsl:with-param>
                    </xsl:call-template>
                    <xsl:call-template name="addFieldGetterToMap">
                        <xsl:with-param name="fieldName">
                            <xsl:value-of select="@Name" /><xsl:text>_Count</xsl:text>
                        </xsl:with-param>
                    </xsl:call-template>
                </xsl:if>
            </xsl:for-each>
            <xsl:text>        return res;</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:if>
        <xsl:if test="$numStates = 1">
            <xsl:text>        return new HashMap&lt;&gt;();</xsl:text>
            <xsl:call-template name="NewLine" />
        </xsl:if>
        <xsl:text>    }&#10;</xsl:text>
    </xsl:template>
    <xsl:template name="addFieldToMap">
        <xsl:param name="fieldName" />
        <xsl:text>        res.put(</xsl:text>
        <xsl:text>getJmxKey() + ":</xsl:text>
        <xsl:value-of select="$fieldName" />
        <xsl:text>", </xsl:text>
        <xsl:value-of select="$fieldName" />
        <xsl:text>.longValue());</xsl:text>
        <xsl:call-template name="NewLine" />
    </xsl:template>
    <xsl:template name="addFieldGetterToMap">
        <xsl:param name="fieldName" />
        <xsl:text>        res.put("</xsl:text>
        <xsl:value-of select="$fieldName" />
        <xsl:text>", get</xsl:text>
        <xsl:value-of select="$fieldName" />
        <xsl:text>());</xsl:text>
        <xsl:call-template name="NewLine" />
    </xsl:template>
</xsl:stylesheet>
