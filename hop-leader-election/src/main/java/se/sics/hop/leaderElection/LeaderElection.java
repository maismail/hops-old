package se.sics.hop.leaderElection;

import se.sics.hop.transaction.handler.LeaderTransactionalRequestHandler;
import se.sics.hop.transaction.handler.LeaderOperationType;
import se.sics.hop.leaderElection.node.ActiveNode;
import se.sics.hop.leaderElection.node.SortedActiveNodeListPBImpl;
import se.sics.hop.leaderElection.node.Node;
import se.sics.hop.leaderElection.node.SortedActiveNodeList;
import se.sics.hop.leaderElection.node.ActiveNodePBImpl;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.exception.TransientStorageException;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.lock.LeaderLockFactory;

/**
 *
 * @author Jude
 * @author Salman Dec 2012. Removed helper classes
 */
public class LeaderElection extends Thread {

  private static final Logger LOG = Logger.getLogger(LeaderElection.class);//LogFactory.getLog(LeaderElection.class);
  public static final long LEADER_INITIALIZATION_ID = -1;
  // interval for monitoring leader
  private final long leadercheckInterval;
  // current Node where the leader election algorithm is running
  protected final Node node;
  protected boolean running;
  // current Leader NN
  protected long leaderId = -1;
  // list of actively running namenodes in the hdfs to be sent to DNs
  protected List<Long> nnList = new ArrayList<Long>();
  private int missedHeartBeatThreshold = 2;
  String hostname;
  String httpAddress;
  boolean suspend = false;

  private Map<Integer, List<HopLeader>> history
          = new HashMap<Integer, List<HopLeader>>();
  private int historyCounter = 0;
  boolean isleader = false;

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public LeaderElection(long leadercheckInterval, int missedHeartBeatThreshold,
          Node node, String httpAddress, String hostName) {
    this.node = node;
    this.leadercheckInterval = leadercheckInterval;
    this.missedHeartBeatThreshold = missedHeartBeatThreshold;
    hostname = hostName;
    this.httpAddress = httpAddress;
    initialize();
  }

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  protected void initialize() {
    // Determine the next leader and set it
    // if this is the leader, also remove previous leaders
    try {
      new LeaderTransactionalRequestHandler(LeaderOperationType.LEADER_ELECTION) {

        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LeaderLockFactory lf = LeaderLockFactory.getInstance();
          locks.add(lf.getVariableLock(HopVariable.Finder.MaxNNID,
                  TransactionLockTypes.LockType.WRITE)).
                  add(lf.getLeaderLock(TransactionLockTypes.LockType.WRITE));
        }

        @Override
        public Object performTask() throws IOException {
          LOG.info(hostname + ") Leader Election initializing ... ");
          determineAndSetLeader();
          return null;
        }
      }.handle();

    } catch (Throwable t) {
      LOG.
              info(hostname
                      + ") LeaderElection thread received Runtime exception. ",
                      t);
      node.stop();
      throw new Error(t);
    }
  }

  /*
   * Determines the leader
   */
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void determineAndSetLeader() throws IOException {
    // Reset the leader (can be the same or new leader)
    leaderId = getLeader();

    // If this node is the leader, 
    if (leaderId == node.getId()) {
      LOG.debug("leaderId " + leaderId + " nn id " + node.getId());
      //remove all previous leaders
      // this block runs when a node turns into a leader node
      // delete all row with id less than the leader id
      if (!node.isLeader() && leaderId != LEADER_INITIALIZATION_ID) {
        LOG.info(hostname + ") New leader elected. Node id: " + node.getId()
                + ", host: "
                + hostname + ", Total Active namenodes in system: "
                + getActiveNodes().size());
      }
      // set role to leader
      isleader = true;
      // dead namenodes from the leader table
      removeDeadNameNodes();
    }
    // TODO [S] do something if i am no longer the leader. 
  }
  private LeaderTransactionalRequestHandler leaderElectionHandler
          = new LeaderTransactionalRequestHandler(
                  LeaderOperationType.LEADER_ELECTION) {

                    @Override
                    public void acquireLock(TransactionLocks locks) throws
                    IOException {
                      LeaderLockFactory lf = LeaderLockFactory.getInstance();
                      locks.add(lf.getVariableLock(HopVariable.Finder.MaxNNID,
                                      TransactionLockTypes.LockType.WRITE))
                      .add(lf.getLeaderLock(TransactionLockTypes.LockType.WRITE));
                    }

                    @Override
                    public Object performTask() throws IOException {
                      updateCounter();
                      // Determine the next leader and set it
                      // if this is the leader, also remove previous leaders
                      determineAndSetLeader();

                      //Update list of active NN in Node.java
                      SortedActiveNodeList sortedList = getActiveNodes();
                      node.setNodesList(sortedList);
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
            running = true;
            while (running && node.isRunning()) {
              try {
                LOG.debug(hostname
                        + ") Leader Election timeout. Updating the counter and checking for new leader "
                        + node.getId() + " " + node.isLeader());
                Long txStartTime = System.currentTimeMillis();

                while (suspend) {
                  try {
                    Thread.sleep(10);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }

                leaderElectionHandler.handle(null/*
                 * fanamesystem
                 */);

                node.setLeader(isleader);
                Long txTotalTime = System.currentTimeMillis() - txStartTime;
                if (txTotalTime < leadercheckInterval) {
                  Thread.sleep(leadercheckInterval - txTotalTime);
                } else {
                  LOG.error(
                          "LeaderElection: Update Tx took very long time to update: "
                          + txTotalTime);
                  Thread.sleep(10); // give a chance to other LE threads. There could be contention
                }

              } catch (InterruptedException ie) {
                LOG.debug(hostname
                        + ") LeaderElection thread received InterruptedException.",
                        ie);
                break;
              } catch (TransientStorageException te){
                LOG.error(hostname 
                  + ") LeaderElection thread received TransientStorageException.",
                        te);
                isleader = false;
                node.setLeader(isleader);
                try {
                  Thread.sleep(leadercheckInterval);
                } catch (InterruptedException ex) {
                  LOG.debug(hostname
                        + ") LeaderElection thread received InterruptedException.",
                        ex);
                  break;
                }
              } catch (Throwable t) {
                LOG.fatal(hostname
                        + ") LeaderElection thread received Runtime exception. ",
                        t);
                node.stop();
                throw new Error(t);
              }
            } // main while loop
          }

          public void finish() {
            running = false;
          }

          public long getLeader() throws IOException {
            List<HopLeader> oldLeaders = history.get(historyCounter
                    - missedHeartBeatThreshold - 1);
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
                    LOG.debug(node.getId() + " newLeader.getId " + newLeader.
                            getId()
                            + " new counter " + newLeader.getCounter()
                            + " old counter " + oldLeader.getCounter()
                            + " histo "
                            + histo + " historyCounter " + historyCounter);
                    if (j < newLeaders.size()) {
                      newLeader = newLeaders.get(j);
                    } else {
                      LOG.error("LeaderElection:" + hostname
                              + ") No alive nodes in the table");
                      throw new Error(hostname
                              + "the leaders table should not only contain dead nodes");
                    }
                  }
                }
              }
              return newLeader.getId();
            }
            LOG.debug(hostname
                    + ") No namenodes in the system. The first one to start would be the leader");
            return LeaderElection.LEADER_INITIALIZATION_ID;
          }

          protected void updateCounter() throws IOException {
            // retrieve counter in [COUNTER] table
            // Use EXCLUSIVE lock
            long counter = getMaxNodeCounter();

            // increment counter
            counter++;
            // Check to see if entry for this NN is in the [LEADER] table
            // May not exist if it was crashed and removed by another leader
            if (!doesNodeExist(node.getId())) {
              long formerId = node.getId();
              node.setId(getNewNamenondeID());
              LOG.info(formerId + " not in the table, setting new id " + node.
                      getId());
              isleader = false;
            }

            // store updated counter in [COUNTER] table
            // hostname is in "ip:port" format
            updateCounter(counter, node.getId(), hostname);

            historyCounter++;
            List<HopLeader> leaders = getAllNameNodes();
            Collections.sort(leaders);
            history.put(historyCounter, leaders);
          }

          long getMaxNodeCounter() throws TransactionContextException,
                  StorageException {
            List<HopLeader> namenodes = getAllNameNodes();
            return getMaxNodeCounter(namenodes);
          }

          List<HopLeader> getAllNameNodes() throws TransactionContextException,
                  StorageException {
            return (List<HopLeader>) EntityManager.
                    findList(HopLeader.Finder.All);
          }

          private long getMaxNodeCounter(List<HopLeader> namenodes) {
            long maxCounter = 0;
            for (HopLeader lRecord : namenodes) {
              if (lRecord.getCounter() > maxCounter) {
                maxCounter = lRecord.getCounter();
              }
            }
            LOG.debug(node.getId() + " max nn counter " + maxCounter
                    + " allnn size "
                    + namenodes.size());
            return maxCounter;
          }

          private boolean doesNodeExist(long leaderId) throws
                  TransactionContextException, StorageException {

            HopLeader leader = EntityManager.find(HopLeader.Finder.ById,
                    leaderId,
                    HopLeader.DEFAULT_PARTITION_VALUE);

            return leader != null;
          }

          private long getNewNamenondeID() throws TransactionContextException,
                  StorageException {
            long newId = Variables.getMaxNNID() + 1;
            Variables.setMaxNNID(newId);
            return newId;
          }

          private void updateCounter(long counter, long id, String hostname)
                  throws
                  IOException {
            // update the counter in [Leader]
            // insert the row. if it exists then update it
            // otherwise create a new row

            HopLeader leader = new HopLeader(id, counter, System.
                    currentTimeMillis(),
                    hostname, httpAddress);
            LOG.debug(hostname + ") Adding/updating row " + leader.toString()
                    + " history counter " + historyCounter);
            EntityManager.add(leader);
          }

          public SortedActiveNodeList getActiveNodes() throws IOException {
            List<HopLeader> nns = new ArrayList<HopLeader>();

            List<HopLeader> oldLeaders = history.get(historyCounter
                    - missedHeartBeatThreshold - 1);
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
            return makeSortedActiveNodeList(nns);
          }

          private void removeDeadNameNodes() throws IOException {
            List<HopLeader> oldLeaders = history.get(historyCounter
                    - missedHeartBeatThreshold - 1);
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
                  LOG.debug(node.getId() + " removing dead node " + oldLeader.
                          getId());
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

          private void removeLeaderRow(HopLeader leader) throws StorageException,
                  TransactionContextException {
            EntityManager.remove(leader);
          }

          // Make sortedNNlist
          private SortedActiveNodeList makeSortedActiveNodeList(
                  List<HopLeader> nns) {
            List<ActiveNode> activeNameNodeList = new ArrayList<ActiveNode>();
            for (HopLeader l : nns) {
              String hostNameNPort = l.getHostName();
              StringTokenizer st = new StringTokenizer(hostNameNPort, ":");
              String intermediaryHostName = st.nextToken();
              int port = Integer.parseInt(st.nextToken());
              st = new StringTokenizer(intermediaryHostName, "/");
              String hostName = st.nextToken();
              String httpAddress = l.getHttpAddress();
              ActiveNode ann = new ActiveNodePBImpl(l.getId(), l.getHostName(),
                      hostName,
                      port, httpAddress);
              activeNameNodeList.add(ann);
            }

            SortedActiveNodeList sortedNNList
                    = new SortedActiveNodeListPBImpl(activeNameNodeList);
            return sortedNNList;
          }

          public void reset() {
            node.setId(-1);
          }

          //for testing
          public void pause() {
            suspend = !suspend;
          }

          public boolean ispaused() {
            return suspend;
          }
}
