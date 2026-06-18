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
package com.aliyun.odps.cupid.trino;


import io.airlift.configuration.Config;

import java.util.ArrayList;
import java.util.List;

public class OdpsConfig
{
    private String project;
    private String accessId;
    private String accessKey;
    private String endpoint;
    private String tunnelEndpoint;
    private int splitSize;
    private List<String> extraProjectList = new ArrayList<>(2);

    public List<String> getExtraProjectList() {
        return extraProjectList;
    }

    public String getProject() {
        return project;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getEndPoint() {
        return endpoint;
    }

    public String getTunnelEndPoint() {
        return tunnelEndpoint;
    }

    public int getSplitSize() {
        return splitSize;
    }

    @Config("odps.project.name.extra.list")
    public OdpsConfig setExtraProjectList(String projectList)
    {
        for (String prj : projectList.split(",")) {
            if (!prj.trim().isEmpty()) {
                this.extraProjectList.add(prj.trim());
            }
        }

        return this;
    }

    @Config("odps.project.name")
    public OdpsConfig setProject(String project)
    {
        this.project = project;
        return this;
    }

    @Config("odps.access.id")
    public OdpsConfig setAccessId(String accessId)
    {
        this.accessId = accessId;
        return this;
    }

    @Config("odps.access.key")
    public OdpsConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @Config("odps.end.point")
    public OdpsConfig setEndPoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @Config("odps.tunnel.end.point")
    public OdpsConfig setTunnelEndPoint(String tunnelEndpoint)
    {
        this.tunnelEndpoint = tunnelEndpoint;
        return this;
    }

    @Config("odps.input.split.size")
    public OdpsConfig setSplitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }
}
