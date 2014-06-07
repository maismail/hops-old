package se.sics.hop.erasure_coding;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;

public class TestDeletion extends BasicClusterTestCase {

  @Test
  public void testCreateEncodedFile() throws IOException {
    FileStatus[] files = getDfs().globStatus(new Path("/*"));
    for (FileStatus file: files) {
      getDfs().delete(file.getPath(), true);
    }

    Path path = new Path("/test_file");
    DistributedFileSystem dfs = getDfs();
    TestUtil.createRandomFile(dfs, path, 0, 1, DFS_TEST_BLOCK_SIZE);

    while(dfs.getEncodingStatus(path.toUri().getPath()).isEncoded() == false) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    dfs.revokeEncoding(path.toUri().getPath(), 3);

    while (true);
  }
}
