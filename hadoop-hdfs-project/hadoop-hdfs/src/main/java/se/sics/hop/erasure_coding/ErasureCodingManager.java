package se.sics.hop.erasure_coding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Daemon;

import java.lang.reflect.Constructor;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class ErasureCodingManager extends Configured{

  static final Log LOG = LogFactory.getLog(ErasureCodingManager.class);

  public static String ENCODING_MANAGER_CLASSNAME_KEY = "se.sics.hop.erasure_coding.encoding_manager";
  public static String BLOCK_REPAIR_MANAGER_CLASSNAME_KEY = "se.sics.hop.erasure_coding.block_rapair_manager";

  private final Namesystem namesystem;
  private final long recheckInterval;
  private EncodingManager encodingManager;
  private BlockRepairManager blockRepairManager;

  final Daemon erasureCodingMonitorThread = new Daemon(new ErasureCodingMonitor());

  public ErasureCodingManager(Namesystem namesystem, Configuration conf) {
    super(conf);
    this.namesystem = namesystem;

    // TODO STEFFEN - Read this from the config file
    this.recheckInterval = 10 * 1000;
  }

  private boolean loadRaidNodeClasses() {
    try {
      Class<?> encodingManagerClass = getConf().getClass(ENCODING_MANAGER_CLASSNAME_KEY, null);
      if (encodingManagerClass == null || !EncodingManager.class.isAssignableFrom(encodingManagerClass)) {
        throw new ClassNotFoundException("Not an implementation of " + EncodingManager.class.getCanonicalName());
      }
      Constructor<?> encodingManagerConstructor = encodingManagerClass.getConstructor(
          new Class[] {Configuration.class, ExecutionResultCallback.class} );
      encodingManager = (EncodingManager) encodingManagerConstructor.newInstance(getConf(), null);

      Class<?> blockRepairManagerClass = getConf().getClass(BLOCK_REPAIR_MANAGER_CLASSNAME_KEY, null);
      if (blockRepairManagerClass == null || !BlockRepairManager.class.isAssignableFrom(blockRepairManagerClass)) {
        throw new ClassNotFoundException("Not an implementation of " + BlockRepairManager.class.getCanonicalName());
      }
      Constructor<?> blockRepairManagerConstructor = blockRepairManagerClass.getConstructor(
          new Class[] {Configuration.class, ExecutionResultCallback.class} );
      blockRepairManager = (BlockRepairManager) blockRepairManagerConstructor.newInstance(getConf(), null);
    } catch (Exception e) {
      LOG.error("Could not load erasure coding classes", e);
      return false;
    }

    return true;
  }

  public void activate() {
    if (!loadRaidNodeClasses()) {
      LOG.error("ErasureCodingMonitor not started. An error occurred during the loading of the encoding library.");
      return;
    }

    erasureCodingMonitorThread.start();
    LOG.info("ErasureCodingMonitor started");
  }

  public void close() {
    try {
      if (erasureCodingMonitorThread != null) {
        erasureCodingMonitorThread.interrupt();
        erasureCodingMonitorThread.join(3000);
      }
    } catch (InterruptedException ie) {
    }
    LOG.info("ErasureCodingMonitor stopped");
  }

  public static boolean isErasureCodingEnabled(Configuration conf) {
    // TODO STEFFEN - Read from config file
    return true;
  }

  private class ErasureCodingMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          if(namesystem.isLeader()){
            // TODO STEFFEN - Do your work
          }
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("ReplicationMonitor thread received InterruptedException.", ie);
          break;
        } /*catch (StorageException e){
          LOG.warn("ReplicationMonitor thread received StorageException.", e);
          break;
        }*/
        catch (Throwable t) {
          LOG.fatal("ReplicationMonitor thread received Runtime exception. ", t);
          terminate(1, t);
        }
      }
    }
  }
}
