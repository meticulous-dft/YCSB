package site.ycsb.workloads;

import java.util.HashMap;
import java.util.Properties;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.WorkloadException;
import site.ycsb.generator.DiscreteGenerator;

public class MongoGlobalWorkload extends CoreWorkload {
  private String shardKey;
  private String locationField;
  private DiscreteGenerator locationChooser;
  private String[] locations = {"US", "EU"};

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    shardKey = p.getProperty("mongodb.shardKey", "user_id");
    locationField = p.getProperty("mongodb.locationField", "location");

    locationChooser = new DiscreteGenerator();
    for (String location : locations) {
      locationChooser.addValue(1.0, location);
    }
  }

  @Override
  protected HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = super.buildValues(key);

    // Add shard key (in this case, it will be the same as the document key)
    values.put(shardKey, new StringByteIterator(key));

    // Add location for zone distribution
    String location = locationChooser.nextString();
    values.put(locationField, new StringByteIterator(location));

    return values;
  }
}
