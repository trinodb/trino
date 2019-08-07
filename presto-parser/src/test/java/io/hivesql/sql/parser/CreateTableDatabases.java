package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class CreateTableDatabases extends SQLTester {

    @Test
    public void testCreateDatabase() {

        String hiveSql = "" +
                "create database if not exists test with dbproperties" +
                "(" +
                "a=true," +
                "c=123," +
                "d=\"dsds\"" +
                ")";
        String prestoSql = "" +
                "create schema if not exists test with " +
                "(" +
                "a=true," +
                "c=123," +
                "d='dsds'" +
                ")";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testDropDatabase() {

        String hiveSql = "drop database if exists test";
        String prestoSql = "drop schema if exists test";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testCreateTable() {

        String hiveSql = "CREATE TABLE tmp.rec_like_event_orc(\n" +
                "  uid bigint, \n" +
                "  video_id bigint, \n" +
                "  dispatch_id string, \n" +
                "  refer string, \n" +
                "  dispatched int, \n" +
                "  displayed int, \n" +
                "  clicked int, \n" +
                "  slide_play int, \n" +
                "  followed int, \n" +
                "  liked int, \n" +
                "  shared int, \n" +
                "  comments int, \n" +
                "  slide int, \n" +
                "  stay int, \n" +
                "  play_second int, \n" +
                "  complete_count int, \n" +
                "  update_timestamp string, \n" +
                "  slide_time int, \n" +
                "  event_time string, \n" +
                "  req_pos int, \n" +
                "  ranker string, \n" +
                "  rough_ranker string, \n" +
                "  score string, \n" +
                "  rough_ranker_score string, \n" +
                "  selector string, \n" +
                "  strategy string, \n" +
                "  check_status string, \n" +
                "  cover_ab string, \n" +
                "  plugin string, \n" +
                "  domain_type string, \n" +
                "  domain_value string, \n" +
                "  abflags_v3 string, \n" +
                "  user_type string, \n" +
                "  filter string, \n" +
                "  os string, \n" +
                "  country string, \n" +
                "  country_region string, \n" +
                "  lng bigint, \n" +
                "  lat bigint, \n" +
                "  net string, \n" +
                "  list_pos int, \n" +
                "  ob1 string, \n" +
                "  ob2 string, \n" +
                "  ob3 string, \n" +
                "  ob4 string, \n" +
                "  ob5 string)\n" +
                "PARTITIONED BY ( \n" +
                "  day string, \n" +
                "  hour string)\n" +
                "ROW FORMAT SERDE \n" +
                "  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' \n";
        String prestoSql = "" +
                "CREATE TABLE tmp.rec_like_event_orc ( \n" +
                "    uid bigint,                             \n" +
                "    video_id bigint,                        \n" +
                "    dispatch_id varchar,                    \n" +
                "    refer varchar,                          \n" +
                "    dispatched integer,                     \n" +
                "    displayed integer,                      \n" +
                "    clicked integer,                        \n" +
                "    slide_play integer,                     \n" +
                "    followed integer,                       \n" +
                "    liked integer,                          \n" +
                "    shared integer,                         \n" +
                "    comments integer,                       \n" +
                "    slide integer,                          \n" +
                "    stay integer,                           \n" +
                "    play_second integer,                    \n" +
                "    complete_count integer,                 \n" +
                "    update_timestamp varchar,               \n" +
                "    slide_time integer,                     \n" +
                "    event_time varchar,                     \n" +
                "    req_pos integer,                        \n" +
                "    ranker varchar,                         \n" +
                "    rough_ranker varchar,                   \n" +
                "    score varchar,                          \n" +
                "    rough_ranker_score varchar,             \n" +
                "    selector varchar,                       \n" +
                "    strategy varchar,                       \n" +
                "    check_status varchar,                   \n" +
                "    cover_ab varchar,                       \n" +
                "    plugin varchar,                         \n" +
                "    domain_type varchar,                    \n" +
                "    domain_value varchar,                   \n" +
                "    abflags_v3 varchar,                     \n" +
                "    user_type varchar,                      \n" +
                "    filter varchar,                         \n" +
                "    os varchar,                             \n" +
                "    country varchar,                        \n" +
                "    country_region varchar,                 \n" +
                "    lng bigint,                             \n" +
                "    lat bigint,                             \n" +
                "    net varchar,                            \n" +
                "    list_pos integer,                       \n" +
                "    ob1 varchar,                            \n" +
                "    ob2 varchar,                            \n" +
                "    ob3 varchar,                            \n" +
                "    ob4 varchar,                            \n" +
                "    ob5 varchar,                            \n" +
                "    day varchar,                            \n" +
                "    hour varchar                            \n" +
                " )                                          \n" +
                " WITH (                                     \n" +
                "    partitioned_by = ARRAY['day','hour'],    \n" +
                "    format = 'ORC'                         \n" +
                " )";
        checkASTNode(prestoSql, hiveSql);
    }

}
