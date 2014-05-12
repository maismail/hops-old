package se.sics.hop.erasure_coding;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.util.Random;

public class TestUtil {

  public static void createRandomFile(DistributedFileSystem dfs, Path path, long seed, int blockCount,
                                      int blockSize) throws IOException {
    FSDataOutputStream out = dfs.create(path);
    byte[] buffer = randomBytes(seed, blockCount, blockSize);
    out.write(buffer, 0, buffer.length);
    out.close();
  }

  public static byte[] randomBytes(long seed, int blockCount, int blockSize) {
    return randomBytes(seed, blockCount * blockSize);
  }

  public static byte[] randomBytes(long seed, int size) {
    final byte[] b = new byte[size];
    final Random rand = new Random(seed);
    rand.nextBytes(b);
    return b;
  }
}
