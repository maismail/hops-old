package se.sics.hop.metadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import org.apache.hadoop.hdfs.server.protocol.SortedActiveNamenodeList;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HopsTransactionalRequestHandler;
import se.sics.hop.transaction.lock.HopsLockFactory;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

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
    private int missedHeartBeatThreshold = 2;
    String hostname;
    String httpAddress;

    private Map<Integer, List<HopLeader>> history = new HashMap<Integer, List<HopLeader>>();
    private int historyCounter = 0;
    //for testing
    boolean suspend = false;

    //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public LeaderElection(Configuration conf, NameNode nn) {
        this.nn = nn;
        this.leadercheckInterval = conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY, DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
        this.missedHeartBeatThreshold = conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY, DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
        hostname = nn.getNameNodeAddress().getAddress().getHostAddress() + ":" + nn.getNameNodeAddress().getPort();
        httpAddress = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT );
        initialize();
    }

    //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    protected void initialize() {
        // Determine the next leader and set it
        // if this is the leader, also remove previous leaders
        try {
          new HopsTransactionalRequestHandler(HDFSOperationType.LEADER_ELECTION) {
            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
              //FIXME: do we need to take write lock on the leader table, it's already safe since we took look on MaxNNID
              HopsLockFactory lf = HopsLockFactory.getInstance();
              locks.add(lf.getVariableLock(HopVariable.Finder.MaxNNID, TransactionLockTypes.LockType.WRITE))
                      .add(lf.getLeaderLock(TransactionLockTypes.LockType.WRITE));
            }

            @Override
            public Object performTask() throws StorageException, IOException {
              LOG.info(hostname + ") Leader Election initializing ... ");
              determineAndSetLeader();
              return null;
            }
          }.handle();
          
            // Start leader election thread
            start();

        } catch (Throwable t) {
            LOG.fatal(hostname + ") LeaderElection thread received Runtime " +
                "exception. ", t);
            nn.stop();
            throw new Error(t);
        }
    }

    /* Determines the leader */
    //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public void determineAndSetLeader() throws IOException,
        StorageException {
        // Reset the leader (can be the same or new leader)
        leaderId = getLeader();

        // If this node is the leader, 
        if (leaderId == nn.getId()) {
            //remove all previous leaders
            // this block runs when a node turns into a leader node
            // delete all row with id less than the leader id
            if (!nn.isLeader() && leaderId != LEADER_INITIALIZATION_ID) {
                LOG.info(hostname + ") New leader elected. Namenode id: " + nn.getId() + ", rpcAddress: " + nn.getServiceRpcAddress() + ", Total Active namenodes in system: " + getActiveNamenodes().size());
            }
            // set role to leader
            // run everitime the determineAndSetLeader in order to reinitiate
            // the lease
            nn.setRole(NamenodeRole.LEADER);
            // dead namenodes from the leader table
            removeDeadNameNodes();
        }
        // TODO [S] do something if i am no longer the leader. 
    }
    private HopsTransactionalRequestHandler leaderElectionHandler = new HopsTransactionalRequestHandler(HDFSOperationType.LEADER_ELECTION) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        HopsLockFactory lf = HopsLockFactory.getInstance();
        locks.add(lf.getVariableLock(HopVariable.Finder.MaxNNID, TransactionLockTypes.LockType.WRITE))
                .add(lf.getLeaderLock(TransactionLockTypes.LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        updateCounter();
        // Determine the next leader and set it
        // if this is the leader, also remove previous leaders
        determineAndSetLeader();

        //Update list of active NN in Namenode.java
        SortedActiveNamenodeList sortedList = getActiveNamenodes();
        nn.setNameNodeList(sortedList);
        while (suspend) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
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
                LOG.debug(hostname + ") Leader Election timeout. Updating the counter and checking for new leader " + nn.getId() + " " + nn.isLeader());
                Long txStartTime = System.currentTimeMillis();
                if (!suspend) {
                    leaderElectionHandler.handle(null/*fanamesystem*/);
                }
                Long txTotalTime = System.currentTimeMillis() - txStartTime;
                if(txTotalTime < leadercheckInterval ){
                  Thread.sleep(leadercheckInterval-txTotalTime);
                }else{
                  LOG.error("LeaderElection: Update Tx took very long time to update");
                  Thread.sleep(10); // give a chance to other LE threads. There could be contention
                }

            } catch (InterruptedException ie) {
                LOG.debug(hostname + ") LeaderElection thread received InterruptedException.", ie);
                break;
            } catch (Throwable t) {
                LOG.fatal(hostname + ") LeaderElection thread received Runtime exception. ", t);
                nn.stop();
                throw new Error(t);
            }
        } // main while loop
    }

    public long getLeader() throws IOException {
        List<HopLeader> oldLeaders = history.get(historyCounter - missedHeartBeatThreshold - 1);
        List<HopLeader> newLeaders = getAllNameNodes();
        Collections.sort(newLeaders);

        if (newLeaders != null && !newLeaders.isEmpty()) {
            if (oldLeaders == null) {
                return newLeaders.get(0).getId();
            }
            int j = 0;
            HopLeader newLeader = newLeaders.get(j);
            for (HopLeader oldLeader : oldLeaders) {
                if (newLeader.getId() == oldLeader.getId()) {
                    if (newLeader.getCounter() > oldLeader.getCounter()) {
                        return oldLeader.getId();
                    } else {
                        j++;
                        long histo = historyCounter - missedHeartBeatThreshold - 1;
                        LOG.debug(nn.getId() + " newLeader.getId " + newLeader.getId() + " new counter " + newLeader.getCounter()
                                + " old counter " + oldLeader.getCounter() + " histo " + histo + " historyCounter " + historyCounter);
                        if (j < newLeaders.size()) {
                            newLeader = newLeaders.get(j);
                        } else {
                            LOG.error("LeaderElection:"+ hostname + ") No alive nodes in the table");
                            throw new Error(hostname + "the leaders table should not only contain dead nodes");
                        }
                    }
                }
            }
            return newLeader.getId();
        }
        LOG.debug(hostname + ") No namenodes in the system. The first one to start would be the leader");
        return LeaderElection.LEADER_INITIALIZATION_ID;
    }

    protected void updateCounter() throws IOException, StorageException {
        // retrieve counter in [COUNTER] table
        // Use EXCLUSIVE lock
        long counter = getMaxNamenodeCounter();

        // increment counter
        counter++;
        // Check to see if entry for this NN is in the [LEADER] table
        // May not exist if it was crashed and removed by another leader
        if (!doesNamenodeExist(nn.getId())) {
            long formerId = nn.getId();
            nn.setId(getNewNamenondeID());
            LOG.debug(formerId + " not in the table, setting new id " + nn.getId());
            nn.setRole(NamenodeRole.NAMENODE);
        }

        // store updated counter in [COUNTER] table
        // hostname is in "ip:port" format
        
        updateCounter(counter, nn.getId(), hostname, httpAddress);

        historyCounter++;
        List<HopLeader> leaders = getAllNameNodes();
        Collections.sort(leaders);
        history.put(historyCounter, leaders);
    }

    long getMaxNamenodeCounter()
        throws StorageException, TransactionContextException {
        List<HopLeader> namenodes = getAllNameNodes();
        return getMaxNamenodeCounter(namenodes);
    }

    List<HopLeader> getAllNameNodes()
        throws StorageException, TransactionContextException {
        return (List<HopLeader>) EntityManager.findList(HopLeader.Finder.All);
    }

    private long getMaxNamenodeCounter(List<HopLeader> namenodes) {
        long maxCounter = 0;
        for (HopLeader lRecord : namenodes) {
            if (lRecord.getCounter() > maxCounter) {
                maxCounter = lRecord.getCounter();
            }
        }
        LOG.debug(nn.getId() + " max nn counter " + maxCounter + " allnn size " + namenodes.size());
        return maxCounter;
    }

    private boolean doesNamenodeExist(long leaderId) throws
        StorageException, TransactionContextException {

        HopLeader leader = EntityManager.find(HopLeader.Finder.ById, leaderId, HopLeader.DEFAULT_PARTITION_VALUE);

        if (leader == null) {
            return false;
        } else {
            return true;
        }
    }

    private long getNewNamenondeID()
        throws StorageException, TransactionContextException {
        long newId = Variables.getMaxNNID() + 1;
        Variables.setMaxNNID(newId);
        return newId;
    }

    private void updateCounter(long counter, long id, String hostname, String httpAddress) throws IOException,
        StorageException {
        // update the counter in [Leader]
        // insert the row. if it exists then update it
        // otherwise create a new row

        HopLeader leader = new HopLeader(id, counter, System.currentTimeMillis(), hostname, httpAddress);
        LOG.debug(hostname + ") Adding/updating row " + leader.toString() + " history counter " + historyCounter);
        EntityManager.add(leader);
    }

    SortedActiveNamenodeList getActiveNamenodes() throws IOException {
        List<HopLeader> nns = new ArrayList<HopLeader>();

        List<HopLeader> oldLeaders = history.get(historyCounter - missedHeartBeatThreshold - 1);
        List<HopLeader> newLeaders = getAllNameNodes();
        Collections.sort(newLeaders);

        int j = 0;
        HopLeader newLeader = newLeaders.get(j);
        if (oldLeaders != null) {
            for (HopLeader oldLeader : oldLeaders) {
                if (newLeader.getId() == oldLeader.getId()) {
                    if (newLeader.getCounter() > oldLeader.getCounter()) {
                        nns.add(oldLeader);
                    }
                    j++;
                    if (j < newLeaders.size()) {
                        newLeader = newLeaders.get(j);
                    } else {
                        break;
                    }
                }
            }
        }
        for (int k = j; k < newLeaders.size(); k++) {
            nns.add(newLeaders.get(k));
        }

        return makeSortedActiveNamenodeList(nns);
    }

    private void removeDeadNameNodes() throws IOException {
        List<HopLeader> oldLeaders = history.get(historyCounter - missedHeartBeatThreshold - 1);
        List<HopLeader> newLeaders = getAllNameNodes();
        Collections.sort(newLeaders);

        if (oldLeaders == null) {
            return;
        }

        int j = 0;
        HopLeader newLeader = newLeaders.get(j);
        for (HopLeader oldLeader : oldLeaders) {
            if (newLeader.getId() == oldLeader.getId()) {
                if (newLeader.getCounter() == oldLeader.getCounter()) {
                    LOG.debug(nn.getId() + " removing deade node " + oldLeader.getId());
                    removeLeaderRow(oldLeader);
                }
                j++;
                if (j < newLeaders.size()) {
                    newLeader = newLeaders.get(j);
                } else {
                    break;
                }
            }
        }
    }

    private void removeLeaderRow(HopLeader leader)
        throws StorageException, TransactionContextException {
        EntityManager.remove(leader);
    }

    // Make sortedNNlist
    private SortedActiveNamenodeList makeSortedActiveNamenodeList(List<HopLeader> nns) {
        List<ActiveNamenode> activeNameNodeList = new ArrayList<ActiveNamenode>();
        for (HopLeader l : nns) {
            String hostNameNPort = l.getHostName();
            StringTokenizer st = new StringTokenizer(hostNameNPort, ":");
            String hostName = st.nextToken();
            int port = Integer.parseInt(st.nextToken());
            String httpAddress = l.getHttpAddress();
            ActiveNamenode ann = new ActiveNamenode(l.getId(), l.getHostName(), hostName, port, httpAddress);
            activeNameNodeList.add(ann);
        }

        SortedActiveNamenodeList sortedNNList = new SortedActiveNamenodeList(activeNameNodeList);
        return sortedNNList;
    }

    //for testing
    void pause() {
        suspend = !suspend;
    }

    boolean ispaused() {
        return suspend;
    }
}
