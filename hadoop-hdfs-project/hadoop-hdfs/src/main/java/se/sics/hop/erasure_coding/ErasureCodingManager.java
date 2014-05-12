package se.sics.hop.erasure_coding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.transaction.handler.EncodingStatusOperationType;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class ErasureCodingManager extends Configured{

  static final Log LOG = LogFactory.getLog(ErasureCodingManager.class);

  public static final String ERASURE_CODING_ENABLED_KEY = "se.sics.hop.erasure_coding.enabled";
  public static final String ENCODING_MANAGER_CLASSNAME_KEY = "se.sics.hop.erasure_coding.encoding_manager";
  public static final String BLOCK_REPAIR_MANAGER_CLASSNAME_KEY = "se.sics.hop.erasure_coding.block_rapair_manager";
  public static final String RECHECK_INTERVAL_KEY = "se.sics.hop.erasure_coding.recheck_interval";
  public static final int DEFAULT_RECHECK_INTERVAL = 1 * 60 * 1000;
  public static final String ACTIVE_ENCODING_LIMIT_KEY = "se.sics.hop.erasure_coding.active_encoding_limit";
  public static final int DEFAULT_ACTIVE_ENCODING_LIMIT = 10;
  public static final String ACTIVE_REPAIR_LIMIT_KEY = "se.sics.hop.erasure_coding.active_repair_limit";
  public static final int DEFAULT_ACTIVE_REPAIR_LIMIT = 10;

  private final FSNamesystem namesystem;
  private final Daemon erasureCodingMonitorThread = new Daemon(new ErasureCodingMonitor());
  private EncodingManager encodingManager;
  private BlockRepairManager blockRepairManager;
  private final long recheckInterval;
  private final int activeEncodingLimit;
  private int activeEncodings = 0;
  private final int activeRepairLimit;
  private int activeRepairs = 0;

  public ErasureCodingManager(FSNamesystem namesystem, Configuration conf) {
    super(conf);
    this.namesystem = namesystem;
    this.recheckInterval = conf.getInt(RECHECK_INTERVAL_KEY, DEFAULT_RECHECK_INTERVAL);
    this.activeEncodingLimit = conf.getInt(ACTIVE_ENCODING_LIMIT_KEY, DEFAULT_ACTIVE_ENCODING_LIMIT);
    this.activeRepairLimit = conf.getInt(ACTIVE_REPAIR_LIMIT_KEY, DEFAULT_ACTIVE_REPAIR_LIMIT);
  }

  private boolean loadRaidNodeClasses() {
    try {
      // TODO STEFFEN - Use ReflectionUtils
      Class<?> encodingManagerClass = getConf().getClass(ENCODING_MANAGER_CLASSNAME_KEY, null);
      if (encodingManagerClass == null || !EncodingManager.class.isAssignableFrom(encodingManagerClass)) {
        throw new ClassNotFoundException("Not an implementation of " + EncodingManager.class.getCanonicalName());
      }
      Constructor<?> encodingManagerConstructor = encodingManagerClass.getConstructor(
          new Class[] {Configuration.class} );
      encodingManager = (EncodingManager) encodingManagerConstructor.newInstance(getConf());

      Class<?> blockRepairManagerClass = getConf().getClass(BLOCK_REPAIR_MANAGER_CLASSNAME_KEY, null);
      if (blockRepairManagerClass == null || !BlockRepairManager.class.isAssignableFrom(blockRepairManagerClass)) {
        throw new ClassNotFoundException("Not an implementation of " + BlockRepairManager.class.getCanonicalName());
      }
      Constructor<?> blockRepairManagerConstructor = blockRepairManagerClass.getConstructor(
          new Class[] {Configuration.class} );
      blockRepairManager = (BlockRepairManager) blockRepairManagerConstructor.newInstance(getConf());
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
    return conf.getBoolean(ERASURE_CODING_ENABLED_KEY, false);
  }

  private class ErasureCodingMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          if(namesystem.isLeader()){
            checkActiveEncodings();
            scheduleEncodings();
            checkActiveRepairs();
            scheduleSourceRepairs();
            scheduleParityRepairs();
          }
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("ErasureCodingMonitor thread received InterruptedException.", ie);
          break;
        }
      }
    }
  }

  private void checkActiveEncodings() {
    List<Report> reports = encodingManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.ENCODED);
          activeEncodings--;
          break;
        case FAILED:
          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.ENCODING_FAILED);
          activeEncodings--;
          break;
        case CANCELED:
          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.ENCODING_CANCELED);
          activeEncodings--;
          break;
      }
    }
  }

  private void updateEncodingStatus(String filePath, EncodingStatus.Status status) {
    try {
      namesystem.updateEncodingStatus(filePath, status);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void scheduleEncodings() {
    final int limit = activeEncodingLimit - activeEncodings;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_ENCODINGS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess = (EncodingStatusDataAccess)
            StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedEncodings(limit);
      }
    };

    try {
      Collection<EncodingStatus> requestedEncodings = (Collection<EncodingStatus>) findHandler.handle();

      for (EncodingStatus encodingStatus : requestedEncodings) {
        INode iNode = namesystem.findInode(encodingStatus.getInodeId());
        if (iNode == null) {
          LOG.error("findInode returned null for id " + encodingStatus.getInodeId());
          continue;
        }
        if (iNode.isUnderConstruction()) {
          // It might still be written to the file
          LOG.info("Still under construction. Encoding not scheduled for " + iNode.getId());
          continue;
        }

        String path = namesystem.getPath(iNode);
        LOG.info("Schedule encoding for " + path);
        encodingManager.encodeFile(encodingStatus.getEncodingPolicy(), new Path(path));
        namesystem.updateEncodingStatus(path, EncodingStatus.Status.ENCODING_ACTIVE);
        activeEncodings++;
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void checkActiveRepairs() {
    List<Report> reports = blockRepairManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          // There should no be a need to update this. The Block manager should trigger this already.
//          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.ENCODED);
          activeRepairs--;
          break;
        case FAILED:
          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.REPAIR_FAILED);
          activeRepairs--;
          break;
        case CANCELED:
          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.REPAIR_CANCELED);
          activeRepairs--;
          break;
      }
    }
  }

  private void scheduleSourceRepairs() {
    final int limit = activeRepairLimit - activeRepairs;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_REPAIRS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess = (EncodingStatusDataAccess)
            StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedRepairs(limit);
      }
    };

    try {
      Collection<EncodingStatus> requestedEncodings = (Collection<EncodingStatus>) findHandler.handle();
      for (EncodingStatus encodingStatus : requestedEncodings) {
        INode iNode = namesystem.findInode(encodingStatus.getInodeId());
        String path = namesystem.getPath(iNode);
        blockRepairManager.repairSourceBlocks(encodingStatus.getEncodingPolicy().getCodec(), new Path(path));
        namesystem.updateEncodingStatus(path, EncodingStatus.Status.REPAIR_ACTIVE);
        activeRepairs++;
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void scheduleParityRepairs() {
    // TODO STEFFEN - Implement parity repair
  }
}
