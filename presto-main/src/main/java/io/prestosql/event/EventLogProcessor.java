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
package io.prestosql.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.prestosql.execution.QueryInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class EventLogProcessor
{
    private static final Logger logger = Logger.get(EventLogProcessor.class);

    private Configuration conf = new Configuration(false);
    private FileSystem fileSystem;
    private final ObjectMapper objectMapper;

    private boolean enableEventLog;
    private String dir;
    private String keytab;
    private String principal;
    private String hadoopConfPath;

    @Inject
    public EventLogProcessor(EventLogConfig config, ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
        this.dir = config.getEventLogDir();
        this.keytab = config.getEventLogKeytab();
        this.principal = config.getEventLogPrincipal();
        this.enableEventLog = config.isEnableEventLog();
        this.hadoopConfPath = config.getHadoopConfPath();
        if (this.enableEventLog) {
            requireNonNull(this.dir, "event-log.dir not set");
            try {
                // default schema: hdfs
                if (dir.startsWith("file")) {
                    initLocalFileSystem();
                }
                else {
                    initHDFSFileSystem();
                }
            }
            catch (IOException e) {
                logger.warn("Please check your event log configuration");
                logger.error(e, e.getMessage());
            }
        }
    }

    public void writeEventLog(QueryInfo queryInfo)
    {
        String sqlId = queryInfo.getQueryId().getId();
        try {
            String event = objectMapper.writeValueAsString(queryInfo);
            Path path = new Path(dir + "/" + sqlId);
            FSDataOutputStream outputStream = fileSystem.create(path);
            outputStream.write(event.getBytes(StandardCharsets.UTF_8));
            outputStream.close();
        }
        catch (IOException e) {
            logger.error(e, e.getMessage());
        }
    }

    public void writeEventLog(String eventLog, String queryId)
    {
        try {
            Path path = new Path(dir + "/" + queryId);
            FSDataOutputStream outputStream = fileSystem.create(path);
            outputStream.write(eventLog.getBytes(StandardCharsets.UTF_8));
            outputStream.close();
        }
        catch (IOException e) {
            logger.error(e, e.getMessage());
        }
    }

    public QueryInfo readQueryInfo(String sqlId)
            throws IOException
    {
        Path path = new Path(dir + "/" + sqlId);
        FSDataInputStream is = fileSystem.open(path);
        String eventLog = inputStreamToString(is);
        return objectMapper.readValue(eventLog, QueryInfo.class);
    }

    private String inputStreamToString(InputStream inputStream)
            throws IOException
    {
        StringBuilder sb = new StringBuilder();
        String line;

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        String str = sb.toString();
        return str;
    }

    private void initLocalFileSystem()
            throws IOException
    {
        fileSystem = FileSystem.get(conf);
    }

    private void initHDFSFileSystem()
            throws IOException
    {
        String hadoopConfPath = requireNonNull(this.hadoopConfPath, "event-log.hadoop.conf.path not set");
        hadoopConfPath = hadoopConfPath.endsWith("/") ? hadoopConfPath : hadoopConfPath + "/";
        conf.addResource(new Path(hadoopConfPath + "core-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "hdfs-site.xml"));

        if (conf.get("hadoop.security.authentication").equalsIgnoreCase("kerberos")) {
            requireNonNull(this.keytab, "event-log.keytab must be set while hadoop.security.authentication = true");
            requireNonNull(this.principal, "event-log.principal must be set while hadoop.security.authentication = true");
            initSecurity();
        }

        fileSystem = FileSystem.get(conf);
        initFolder();
    }

    private void initFolder()
            throws IOException
    {
        if (!fileSystem.exists(new Path(dir))) {
            fileSystem.mkdirs(new Path(dir));
            logger.info("Init path: " + dir);
        }
    }

    private void initSecurity()
            throws IOException
    {
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);

        logger.info("Login " + principal + " from " + keytab);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }

    public boolean isEnableEventLog()
    {
        return enableEventLog;
    }
}
