package se.sics.hop.erasure_coding;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class TestErasureCodingApi extends BasicClusterTestCase {

  private final Path testFile = new Path("/test_file");

  @Test
  public void testCreateEncodedFile() throws IOException {
    FSDataOutputStream out = getDfs().create(testFile);
    out.close();

    assertNotNull(getDfs().getEncodingStatus(testFile.toUri().getPath()));
  }

  @Test
  public void testGetEncodingStatusIfRequested() throws IOException {
    Codec codec = Codec.getCodec("src");
    EncodingPolicy policy = new EncodingPolicy(codec.getId(), (short) 1);
    FSDataOutputStream out = getDfs().create(testFile, policy);
    out.close();

    EncodingStatus status = getDfs().getEncodingStatus(testFile.toUri().getPath());
    assertNotNull(status);
    assertEquals(EncodingStatus.Status.ENCODING_REQUESTED, status.getStatus());
  }

  public void testGetEncodingStatusForNonExistingFile() throws IOException {
    try {
      getDfs().getEncodingStatus("/DEAD_BEEF");
      fail();
    } catch (IOException e) {

    }
  }

  @Test
  public void testGetEncodingStatusIfNotRequested() throws IOException {
    FSDataOutputStream out = getDfs().create(testFile);
    out.close();

    EncodingStatus status = getDfs().getEncodingStatus(testFile.toUri().getPath());
    assertNotNull(status);
    assertEquals(EncodingStatus.Status.NOT_ENCODED, status.getStatus());
  }
}
