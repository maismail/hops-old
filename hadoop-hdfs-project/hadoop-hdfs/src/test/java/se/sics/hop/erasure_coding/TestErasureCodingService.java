package se.sics.hop.erasure_coding;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.lang.reflect.Constructor;

public class TestErasureCodingService extends TestCase {

  public static String RAIDNODE_CLASSNAME_KEY = "raidnode.classname";

  @Test
  public void testLoadEncodingManager() throws ClassNotFoundException,
      NoSuchMethodException {
    Configuration conf = new Configuration();
    conf.set(RAIDNODE_CLASSNAME_KEY, "se.sics.hop.erasure_coding" +
        ".LocalEncodingManagerImpl");

    Class<?> raidNodeClass =
        conf.getClass(RAIDNODE_CLASSNAME_KEY, null);
    if (!EncodingManager.class.isAssignableFrom(raidNodeClass)) {
      throw new ClassNotFoundException("not an implementation of" +
          "ErasureCodingService");
    }
    Constructor<?> constructor =
        raidNodeClass.getConstructor(new Class[] {Configuration.class,
            EncodingStatusCallback.class} );
  }
}
