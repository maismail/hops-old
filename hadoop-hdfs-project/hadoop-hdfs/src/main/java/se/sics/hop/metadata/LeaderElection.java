package se.sics.hop.metadata;

import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import org.apache.hadoop.hdfs.server.protocol.SortedActiveNamenodeList;

/**
 *
 * @author Jude
 * @author Salman Dec 2012. Removed helper classes
 */
public class LeaderElection extends Thread {

  private static final Log LOG = LogFactory.getLog(LeaderElection.class);
  public static final long LEADER_INITIALIZATION_ID = -1;
  // interval for monitoring leader
  private final long leadercheckInterval;
  // current Namenode where the leader election algorithm is running
  protected final NameNode nn;
  // current Leader NN
  protected long leaderId = -1;
  // list of actively running namenodes in the hdfs to be sent to DNs
  protected List<Long> nnList = new ArrayList<Long>();
  private int missedHeartBeatThreshold = 1;
  String hostname;
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public LeaderElection(Configuration conf, NameNode nn) {
    this.nn = nn;
    this.leadercheckInterval = conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY, DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
    this.missedHeartBeatThreshold = conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY, DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
    hostname = nn.getNameNodeAddress().getAddress().getHostAddress() + ":" + nn.getNameNodeAddress().getPort();
    initialize();
  }

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  protected void initialize() {
    // Determine the next leader and set it
    // if this is the leader, also remove previous leaders
    try {
      new HDFSTransactionalRequestHandler(HDFSOperationType.LEADER_ELECTION) {

        @Override
        public TransactionLocks acquireLock() throws PersistanceException, IOException {
          HDFSTransactionLockAcquirer  tla = new HDFSTransactionLockAcquirer();
          tla.getLocks().addLeaderLock(TransactionLockTypes.LockType.WRITE);
          return tla.acquireLeaderLock();
        }

        @Override
        public Object performTask() throws PersistanceException, IOException {
          LOG.info(hostname+") Leader Election initializing ... ");
          determineAndSetLeader();
          return null;
        }
      }.handle();

      // Start leader election thread
      start();

    } catch (Throwable t) {
      LOG.info(hostname+") LeaderElection thread received Runtime exception. ", t);
      nn.stop();
      Runtime.getRuntime().exit(-1);
    }
  }

  /* Determines the leader */
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void determineAndSetLeader() throws IOException, PersistanceException {
    // Reset the leader (can be the same or new leader)
    leaderId = getLeader();

    // If this node is the leader, remove all previous leaders
    // this block runs when a node turns into a leader node
    // delete all row with id less than the leader id
    // set role to leader
    if (leaderId == nn.getId() && !nn.isLeader() && leaderId != LEADER_INITIALIZATION_ID) {
      LOG.info(hostname+") New leader elected. Namenode id: " + nn.getId() + ", rpcAddress: " + nn.getServiceRpcAddress() + ", Total Active namenodes in system: " + selectAll().size());
      // remove all previous leaders from [LEADER] table
      removePrevoiouslyElectedLeaders(leaderId);
      nn.setRole(NamenodeRole.LEADER);
    }
    
    // if leader then remove dead namenodes from the leader table
//    if(nn.getRole() == NamenodeRole.LEADER)
//    {
//      removeDeadNameNodes();
//    }
    // TODO [S] do something if i am no longer the leader. 
  }
  private HDFSTransactionalRequestHandler leaderElectionHandler = new HDFSTransactionalRequestHandler(HDFSOperationType.LEADER_ELECTION) {

    @Override
    public TransactionLocks acquireLock() throws PersistanceException, IOException {
      HDFSTransactionLockAcquirer  tla = new HDFSTransactionLockAcquirer();
      tla.getLocks().addLeaderLock(TransactionLockTypes.LockType.WRITE);
      return tla.acquireLeaderLock();
    }

    @Override
    public Object performTask() throws PersistanceException, IOException {
      updateCounter();
      // Determine the next leader and set it
      // if this is the leader, also remove previous leaders
      determineAndSetLeader();
      
      //Update list of active NN in Namenode.java
      SortedActiveNamenodeList sortedList = getActiveNamenodes();
      nn.setNameNodeList(sortedList);
      return null;
    }
  };

  @Override
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void run() {
    // Leader election algorithm works in one transaction in each round. Reading all rows 
    // with read-committed lock should be fine. Because every NN only updates its row, and
    // the potential conflict could be when there are two leaders in the system try to delete
    // preceding rows which could end up in deadlock. 
    // we solved it by making sure that the namenodes delete the rows in the same order.
    
    while (nn.getNamesystem().isRunning()) {
      try {
//        LOG.info(hostname+") Leader Election timeout. Updating the counter and checking for new leader");
        leaderElectionHandler.handle(null/*fanamesystem*/);
        Thread.sleep(leadercheckInterval);

      } catch (InterruptedException ie) {
        LOG.info(hostname+") LeaderElection thread received InterruptedException.", ie);
        break;
      } catch (Throwable t) {
        LOG.fatal(hostname+") LeaderElection thread received Runtime exception. ", t);
        nn.stop();
        Runtime.getRuntime().exit(-1);
      }
    } // main while loop
  }

  public long getLeader() throws IOException, PersistanceException {
    long maxCounter = getMaxNamenodeCounter();
    long totalNamenodes = getLeaderRowCount();
    if (totalNamenodes == 0) {
      LOG.info(hostname+") No namenodes in the system. The first one to start would be the leader");
      return LeaderElection.LEADER_INITIALIZATION_ID;
    }

    List<HopLeader> activeNamenodes = getActiveNamenodesInternal(maxCounter, totalNamenodes);
    return getMinNamenodeId(activeNamenodes);
  }

  protected void updateCounter() throws IOException, PersistanceException {
    // retrieve counter in [COUNTER] table
    // Use EXCLUSIVE lock
    long counter = getMaxNamenodeCounter();

    // increment counter
    counter++;
    // Check to see if entry for this NN is in the [LEADER] table
    // May not exist if it was crashed and removed by another leader
    if (!doesNamenodeExist(nn.getId())) {
      nn.setId(getMaxNamenodeId() + 1);
    }

    // store updated counter in [COUNTER] table
    // hostname is in "ip:port" format
    updateCounter(counter, nn.getId(), hostname);
  }

  /* The function that returns the list of actively running NNs */
  //--------------------------------------------------------------------------------------------------------------
  SortedActiveNamenodeList selectAll() throws IOException, PersistanceException {
    return getActiveNamenodes();
  }

  private long getMaxNamenodeCounter() throws PersistanceException {
    List<HopLeader> namenodes = getAllNamenodesInternal();
    return getMaxNamenodeCounter(namenodes);
  }

  private List<HopLeader> getAllNamenodesInternal() throws PersistanceException {

    List<HopLeader> leaders = (List<HopLeader>) EntityManager.findList(HopLeader.Finder.All);
    return leaders;
  }

  private long getMaxNamenodeCounter(List<HopLeader> namenodes) {
    long maxCounter = 0;
    for (HopLeader lRecord : namenodes) {
      if (lRecord.getCounter() > maxCounter) {
        maxCounter = lRecord.getCounter();
      }
    }
    return maxCounter;
  }

  private boolean doesNamenodeExist(long leaderId) throws PersistanceException {

    HopLeader leader = EntityManager.find(HopLeader.Finder.ById, leaderId, HopLeader.DEFAULT_PARTITION_VALUE);

    if (leader == null) {
      return false;
    } else {
      return true;
    }
  }

  public long getMaxNamenodeId() throws PersistanceException {
    List<HopLeader> namenodes = getAllNamenodesInternal();
    return getMaxNamenodeId(namenodes);
  }

  private static long getMaxNamenodeId(List<HopLeader> namenodes) {
    long maxId = 0;
    for (HopLeader lRecord : namenodes) {
      if (lRecord.getId() > maxId) {
        maxId = lRecord.getId();
      }
    }
    return maxId;
  }

  private void updateCounter(long counter, long id, String hostname) throws IOException, PersistanceException {
    // update the counter in [Leader]
    // insert the row. if it exists then update it
    // otherwise create a new row
    
    HopLeader leader = new HopLeader(id, counter, System.currentTimeMillis(), hostname);
//    LOG.info(hostname+") Adding/updating row "+leader.toString());
    EntityManager.add(leader);
  }

  private long getLeaderRowCount() throws IOException, PersistanceException {
    return EntityManager.count(HopLeader.Counter.All);
  }
  
  private List<HopLeader> getAllNameNodes() throws IOException, PersistanceException {
    return (List<HopLeader>)EntityManager.findList(HopLeader.Finder.All);
  }

  private List<HopLeader> getActiveNamenodesInternal(long counter, long totalNamenodes) throws PersistanceException {
    long condition = counter - totalNamenodes * missedHeartBeatThreshold;
    List<HopLeader> list = (List<HopLeader>) EntityManager.findList(HopLeader.Finder.AllByCounterGTN, condition);
    return list;
  }

  private static long getMinNamenodeId(List<HopLeader> namenodes) {
    long minId = Long.MAX_VALUE;
    for (HopLeader record : namenodes) {
      if (record.getId() < minId) {
        minId = record.getId();
      }
    }
    return minId;
  }

  public SortedActiveNamenodeList getActiveNamenodes() throws PersistanceException, IOException {
    // get max counter and total namenode count
    long maxCounter = getLeaderRowCount();
    int totalNamenodes = getAllNamenodesInternal().size();

    // get all active namenodes
    List<HopLeader> nns = getActiveNamenodesInternal(maxCounter, totalNamenodes);

    return makeSortedActiveNamenodeList(nns);
  }

  public void removePrevoiouslyElectedLeaders(long id) throws PersistanceException {
    List<HopLeader> prevLeaders = getPreceedingNamenodesInternal(id);
    // Sort the leaders based on the id to avoid the scenario that there are two NNs
    // as leaders and both want to remove all preceding leaders which could result to
    // deadlock.
    Collections.sort(prevLeaders);
    for (HopLeader l : prevLeaders) {
      removeLeaderRow(l);
    }
  }
  
  private void removeDeadNameNodes() throws PersistanceException, IOException {
    long maxCounter = getMaxNamenodeCounter();
    long totalNamenodes = getLeaderRowCount();
    if (totalNamenodes == 0) {
      // no rows, nothing to delete
      return;
    }

    List<HopLeader> activeNameNodes = getActiveNamenodesInternal(maxCounter, totalNamenodes);
    List<HopLeader> allNameNodes = getAllNameNodes();

    deleteInactiveNameNodes(allNameNodes, activeNameNodes);

  }

  private void deleteInactiveNameNodes(List<HopLeader> allNameNodes, List<HopLeader> activeNameNodes)
          throws PersistanceException {
    for (HopLeader leader : allNameNodes) {
      if (!activeNameNodes.contains(leader)) {
        removeLeaderRow(leader);
      }
    }

  }

  private void removeLeaderRow(HopLeader leader) throws PersistanceException {
    EntityManager.remove(leader);
  }

  private List<HopLeader> getPreceedingNamenodesInternal(long id) throws PersistanceException {
    List<HopLeader> list = (List<HopLeader>) EntityManager.findList(HopLeader.Finder.AllByIDLT, id);
    return list;
  }
  
  
 // Make sortedNNlist
  private SortedActiveNamenodeList makeSortedActiveNamenodeList(List<HopLeader> nns){
    List<ActiveNamenode> activeNameNodeList = new ArrayList<ActiveNamenode>();    
    for (HopLeader l : nns) {
      String hostNameNPort = l.getHostName();
      StringTokenizer st = new StringTokenizer(hostNameNPort, ":");
      String hostName = st.nextToken();
      int port = Integer.parseInt(st.nextToken());
      ActiveNamenode ann = new ActiveNamenode(l.getId(), l.getHostName(), hostName, port);
      activeNameNodeList.add(ann);
    }
    
    SortedActiveNamenodeList sortedNNList = new SortedActiveNamenodeList(activeNameNodeList);
    return sortedNNList;
  }
}
