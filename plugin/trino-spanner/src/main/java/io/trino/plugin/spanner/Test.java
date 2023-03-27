package io.trino.plugin.spanner;

import com.google.cloud.spanner.SpannerApiFutures;
import com.google.spanner.v1.SpannerGrpc;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;

public class Test
{
    public static void main(String[] args)
            throws SQLException
    {
        Connection connection = DriverManager.getConnection("jdbc:cloudspanner://0.0.0.0:9010/projects/spanner-project/instances/spanner-instance/databases/spanner-database;autoConfigEmulator=true");
        Statement statement = connection.createStatement();
        statement.execute("drop table t1");
        //1679963300421000
        //1591142320347
        //2020-06-02T23:58:40.347847393Z
        Instant parse = Instant.parse("2020-06-02T23:58:40.347847393Z");
        //2023-03-28T00:42:31.756+00:00
        System.out.println(parse.toEpochMilli());
        System.out.println(Instant.ofEpochMilli(1679963300421L));
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(1679963300421000L, 0, ZoneOffset.UTC);
        System.out.println(localDateTime);
        statement.execute("create table t1 (id int64,ts TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true))PRIMARY KEY (id)");
        statement.execute("select * from ");
        /*PreparedStatement preparedStatement = connection.prepareStatement("insert into t1 values(?,?)");
        preparedStatement.setInt(1,1);
        preparedStatement.setTimestamp(1,1);*/
    }
}
