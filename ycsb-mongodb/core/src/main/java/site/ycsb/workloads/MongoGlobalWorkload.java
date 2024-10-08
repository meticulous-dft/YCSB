package site.ycsb.workloads;

import java.util.HashMap;
import java.util.Properties;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.WorkloadException;

public class MongoGlobalWorkload extends CoreWorkload {
  private String shardKey;
  private String location;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    shardKey = p.getProperty("mongodb.shardKey", "user_id");
    location = p.getProperty("mongodb.location", "US");
  }

  @Override
  protected HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = super.buildValues(key);

    // Add shard key (in this case, it will be the same as the document key)
    values.put(shardKey, new StringByteIterator(key));

    // Add location for zone distribution
    values.put("location", new StringByteIterator(location));

    return values;
  }

  @Override
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {}
}
