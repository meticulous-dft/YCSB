package site.ycsb.workloads;

import java.util.HashMap;
import java.util.Objects;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

public class MongoGlobalWorkload extends CoreWorkload {

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
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();

    if (!cells.isEmpty()) {
      if (location.isEmpty() || !Objects.equals(cells.get("location").toString(), location)) {
        System.err.println(
            "Error verifying location: expect "
                + location
                + " get "
                + cells.get("location").toString());
        verifyStatus = Status.UNEXPECTED_STATE;
      }
      if (shardKey.isEmpty() || !Objects.equals(cells.get(shardKey).toString(), key)) {
        System.err.println(
            "Error verifying shard key: expect " + key + " get " + cells.get(shardKey).toString());
        verifyStatus = Status.UNEXPECTED_STATE;
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
  }
}
