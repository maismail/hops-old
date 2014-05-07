package se.sics.hop.erasure_coding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Daemon;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.lock.ErasureCodingTransactionLockAcquirer;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.EncodingStatusOperationType;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.transaction.handler.TransactionalRequestHandler;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

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
  public static final int DEFAULT_RECHECK_INTERVAL = 10 * 60 * 1000;
  public static final String ACTIVE_ENCODING_LIMIT_KEY = "se.sics.hop.erasure_coding.active_encoding_limit";
  public static final int DEFAULT_ACTIVE_ENCODING_LIMIT = 10;

  private final Namesystem namesystem;
  private final Daemon erasureCodingMonitorThread = new Daemon(new ErasureCodingMonitor());
  private EncodingManager encodingManager;
  private BlockRepairManager blockRepairManager;
  private final long recheckInterval;
  private final int activeEncodingLimit;
  private int activeEncodings = 0;

  public ErasureCodingManager(Namesystem namesystem, Configuration conf) {
    super(conf);
    this.namesystem = namesystem;
    this.recheckInterval = conf.getInt(RECHECK_INTERVAL_KEY, DEFAULT_RECHECK_INTERVAL);
    this.activeEncodingLimit = conf.getInt(ACTIVE_ENCODING_LIMIT_KEY, DEFAULT_ACTIVE_ENCODING_LIMIT);
  }

  private boolean loadRaidNodeClasses() {
    try {
      Class<?> encodingManagerClass = getConf().getClass(ENCODING_MANAGER_CLASSNAME_KEY, null);
      if (encodingManagerClass == null || !EncodingManager.class.isAssignableFrom(encodingManagerClass)) {
        throw new ClassNotFoundException("Not an implementation of " + EncodingManager.class.getCanonicalName());
      }
      Constructor<?> encodingManagerConstructor = encodingManagerClass.getConstructor(
          new Class[] {Configuration.class} );
      encodingManager = (EncodingManager) encodingManagerConstructor.newInstance(getConf(), null);

      Class<?> blockRepairManagerClass = getConf().getClass(BLOCK_REPAIR_MANAGER_CLASSNAME_KEY, null);
      if (blockRepairManagerClass == null || !BlockRepairManager.class.isAssignableFrom(blockRepairManagerClass)) {
        throw new ClassNotFoundException("Not an implementation of " + BlockRepairManager.class.getCanonicalName());
      }
      Constructor<?> blockRepairManagerConstructor = blockRepairManagerClass.getConstructor(
          new Class[] {Configuration.class} );
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
            scheduleRepairs();
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

  private void checkActiveEncodings() {
    List<Report> reports = encodingManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          break;
        case FAILED:
          break;
        case CANCELED:
          break;
      }
    }
  }

  private void scheduleEncodings() {
    int limit = activeEncodingLimit - activeEncodings;
    if (limit <= 0) {
      return;
    }

    try {
      Collection<EncodingStatus> requestedEncodings = EntityManager.findList(
          EncodingStatus.Finder.LimitedByStatusRequestEncodings, limit);

      for (EncodingStatus encodingStatus : requestedEncodings) {
        // TODO STEFFEN - Check if file was completely written yet
        INode iNode = findInode(encodingStatus.getInodeId());
        encodingManager.encodeFile(encodingStatus.getCodec(), new Path(iNode.getFullPathName()));
      }
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  private INode findInode(final long id) throws IOException {
    HDFSTransactionalRequestHandler findInodeHandler =
        new HDFSTransactionalRequestHandler(HDFSOperationType.GET_INODE) {

      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
        // TODO STEFFEN - Is this the right lock?
        tla.getLocks().addINode(TransactionLockTypes.INodeLockType.READ);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return EntityManager.find(INode.Finder.ByINodeID, id);
      }
    };
    return (INode) findInodeHandler.handle(this);
  }

  private void checkActiveRepairs() {
    List<Report> reports = blockRepairManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          break;
        case FAILED:
          break;
        case CANCELED:
          break;
      }
    }
  }

  private void scheduleRepairs() {

  }
}
