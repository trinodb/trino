package io.prestosql.plugin.hive.metastore.alluxio;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

public class TestAlluxioHiveMetastoreConfig {

  @Test
  public void testDefaults()
  {
    assertRecordedDefaults(recordDefaults(AlluxioHiveMetastoreConfig.class)
        .setMasterAddress(null));
  }

  @Test
  public void testExplicitPropertyMapping()
  {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
        .put("hive.metastore.alluxio.master.address", "localhost:19998")
        .build();

    AlluxioHiveMetastoreConfig expected = new AlluxioHiveMetastoreConfig()
        .setMasterAddress("localhost:19998");

    assertFullMapping(properties, expected);
  }
}
