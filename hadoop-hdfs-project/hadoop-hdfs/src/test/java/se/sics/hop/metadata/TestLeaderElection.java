/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.metadata;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author gautier
 */
public class TestLeaderElection {

    private static final Log LOG = LogFactory.getLog(TestLeaderElection.class);
    HdfsConfiguration conf = null;
    List<NameNode> nnList;

    @Before
    public void init() throws StorageInitializtionException, StorageException, IOException {
//        forbidSystemExitCall();
        conf = new HdfsConfiguration();
        nnList = new ArrayList<NameNode>();
        StorageFactory.setConfiguration(conf);
        StorageFactory.getConnector().formatStorage();

        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1");
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1");
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r2");
        DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d1/r2");
        DFSTestUtil.getDatanodeDescriptor("5.5.5.5", "/d2/r3");
        DFSTestUtil.getDatanodeDescriptor("6.6.6.6", "/d2/r3");

        FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
        conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
        File baseDir = new File(System.getProperty(
                "test.build.data", "build/test/data"), "dfs/");
        conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
                new File(baseDir, "name").getPath());

        conf.setBoolean(
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
        conf.setBoolean(
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);

        //Gautier conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 10);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 10);
        conf.setInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY, 11 * 1000);
        conf.setInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY, 2);

        DFSTestUtil.formatNameNode(conf);
    }

    @After
    public void tearDown() {
        //stop all NN
        LOG.debug("tearDown");
        for (NameNode nn : nnList) {
            nn.stop();
        }
    }

    /**
     * Test if the NN are correctly removed from the Active NN list
     */
    @Test
    public void testDeadNodesRemoval() throws IOException, InterruptedException {
        LOG.debug("start testDeadNodesRemoval");
        List<InetSocketAddress> isaList = new ArrayList<InetSocketAddress>();
        //create 10 NN
        for (int i = 0; i < 10; i++) {
//            conf.set(FS_DEFAULT_NAME_KEY, "localhost:5861" + i);
            conf.set(FS_DEFAULT_NAME_KEY, "localhost:0");
            conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:0");
            NameNode nn = new NameNode(conf);
            nnList.add(nn);
            isaList.add(nn.getNameNodeAddress());
        }
        //verify that the number of active nn is equal to the number of started NN
        List<ActiveNamenode> activesNNs = getActiveNN(nnList.get(0).getLeaderElectionInstance());
        assertTrue("wrong number of active NN " + activesNNs.size(), activesNNs.size() == nnList.size());
        //verify that there is one and only one leader.
        int leaderId = 0;
        int nbLeaders = 0;
        for (int i = 0; i < nnList.size(); i++) {
            if (nnList.get(i).isLeader()) {
                nbLeaders++;
                leaderId = i;
            }
        }
        assertTrue("there is no leader", nbLeaders > 0);
        assertTrue("there is more than one leader", nbLeaders == 1);

        //stop the leader
        nnList.get(leaderId).stop();

        Thread.sleep(40000);

        //verify that there is one and only one leader.
        int newLeaderId = 0;
        nbLeaders = 0;
        for (int i = 0; i < nnList.size(); i++) {
            if (i != leaderId) {
                if (nnList.get(i).isLeader()) {
                    nbLeaders++;
                    newLeaderId = i;
                }
            }
        }
        assertTrue("there is no leader", nbLeaders > 0);
        assertTrue("there is more than one leader", nbLeaders == 1);

        //verify that the stoped leader is not in the active list anymore
        activesNNs = getActiveNN(nnList.get(newLeaderId).getLeaderElectionInstance());
        for (ActiveNamenode ann : activesNNs) {
            assertFalse("previous is stil in active nn", ann.getInetSocketAddress().equals(isaList.get(leaderId)));
        }

        //stop NN last alive NN
        int tokill = nnList.size() - 1;
        if (leaderId == tokill) {
            tokill--;
        }
        nnList.get(tokill).stop();
        Thread.sleep(40000);

        //verify that the killed NN is not in the active NN list anymore
        activesNNs = getActiveNN(nnList.get(1).getLeaderElectionInstance());
        for (ActiveNamenode ann : activesNNs) {
            assertFalse("killed nn is stil in active nn", ann.getInetSocketAddress().equals(isaList.get(tokill)));
        }

    }

    /**
     *
     */
    @Test
    public void testSlowLeader() throws IOException, InterruptedException {
        LOG.debug("start testSlowLeader");
        //create 10 NN
        for (int i = 0; i < 10; i++) {
            conf.set(FS_DEFAULT_NAME_KEY, "localhost:0");
            conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:0");
            NameNode nn = new NameNode(conf);
            nnList.add(nn);
        }
        //verify that the number of active nn is equal to the number of started NN
        List<ActiveNamenode> activesNNs = getActiveNN(nnList.get(0).getLeaderElectionInstance());
        assertTrue("wrong number of actives NN " + activesNNs.size(), activesNNs.size() == nnList.size());
        //verify that there is one and only one leader.
        int leaderId = 0;
        int nbLeaders = 0;
        for (int i = 0; i < nnList.size(); i++) {
            if (nnList.get(i).isLeader()) {
                nbLeaders++;
                leaderId = i;
            }
        }
        assertTrue("there is no leader", nbLeaders > 0);
        assertTrue("there is more than one leader", nbLeaders == 1);

        //slowdown leader NN by suspending its thread during 10s 
        nnList.get(leaderId).getLeaderElectionInstance().pause();

        Thread.sleep(40000);

        nnList.get(leaderId).getLeaderElectionInstance().pause();

        //verify that there is one and only one leader.
        nbLeaders = 0;
        for (int i = 0; i < nnList.size(); i++) {
            if (nnList.get(i).isLeader()) {
                nbLeaders++;
                LOG.debug("leader is " + nnList.get(i).getId());
            }
        }
        assertTrue("there is no leader", nbLeaders > 0);
        assertTrue("there is more than one leader", nbLeaders == 1);

    }

    private static void forbidSystemExitCall() {
        final SecurityManager securityManager = new SecurityManager() {
            @Override
            public void checkPermission(Permission permission) {
                if (permission.getName().startsWith("exitVM")) {
                    throw new RuntimeException("Something called exit ");
                }
            }
        };
        System.setSecurityManager(securityManager);
    }

    //TODO churn
    /**
     *
     */
    @Test
    public void testChurn() throws IOException, InterruptedException {
        LOG.debug("start testChurn");
        Random rand = new Random(0);
        List<NameNode> activNNList = new ArrayList<NameNode>();
        List<NameNode> stopedNNList = new ArrayList<NameNode>();
        int nbStartedNodes = 0;
        //create 10 NN
        for (int i = 0; i < 10; i++) {
            nbStartedNodes++;
            conf.set(FS_DEFAULT_NAME_KEY, "localhost:0");
            conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:0");
            NameNode nn = new NameNode(conf);
            nnList.add(nn);
            activNNList.add(nn);
        }
        Thread.sleep(5000);
        //verify that there is one and only one leader.
        int nbLeaders = 0;
        for (NameNode nn : nnList) {
            if (nn.isLeader()) {
                nbLeaders++;
            }
        }
        assertTrue("there is no leader", nbLeaders > 0);
        assertTrue("there is more than one leader", nbLeaders == 1);

        long startingTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startingTime < 10 * 60 * 1000) {
            //stop random number of random NN
            int nbStop = rand.nextInt(activNNList.size() - 1);
            for (int i = 0; i < nbStop; i++) {
                int nnId = rand.nextInt(activNNList.size());
//                LOG.debug("suspend node with id " + activNNList.get(nnId).getId());
                activNNList.get(nnId).getLeaderElectionInstance().pause();
                stopedNNList.add(activNNList.get(nnId));
                activNNList.remove(nnId);
            }
//            LOG.debug("suspended " + nbStop + " nodes");

            //start random number of new NN
            int nbStart = rand.nextInt(10);
            if (nbStartedNodes + nbStart > 1000) {
                nbStart = 100 - nbStartedNodes;
            }
            for (int i = 0; i < nbStart; i++) {
                nbStartedNodes++;
                conf.set(FS_DEFAULT_NAME_KEY, "localhost:0");
                conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:0");
                NameNode nn = new NameNode(conf);
                nnList.add(nn);
                activNNList.add(nn);
            }
            //restart a random number of stoped NN
            int nbRestart = rand.nextInt(stopedNNList.size());
            for (int i = 0; i < nbRestart; i++) {
                int nnId = rand.nextInt(stopedNNList.size());
//                LOG.debug("resum node with id " + stopedNNList.get(nnId).getId());
                stopedNNList.get(nnId).getLeaderElectionInstance().pause();
                activNNList.add(stopedNNList.get(nnId));
                stopedNNList.remove(nnId);
            }

            //verify that there is at most one leader.
            nbLeaders = 0;

            for (NameNode nn : nnList) {
                if (nn.isLeader()) {
                    nbLeaders++;
                }
            }

            if (nbLeaders > 1) {
                assertTrue("there is more than one leader " + nbLeaders, nbLeaders <= 1);
            }
            //TODO verify that the time without leader is bound 
        }

        //restart all NN in order to stop them properly 
        for (NameNode nn : stopedNNList) {
//            LOG.debug("resum node with id " + nn.getId());
            nn.getLeaderElectionInstance().pause();
        }

    }

    private List<ActiveNamenode> getActiveNN(final LeaderElection leaderElector) throws IOException {
        return (List<ActiveNamenode>) new HDFSTransactionalRequestHandler(HDFSOperationType.LEADER_ELECTION) {

            @Override
            public TransactionLocks acquireLock() throws PersistanceException, IOException {
                HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
                tla.getLocks().addLeaderTocken(TransactionLockTypes.LockType.WRITE);
                tla.getLocks().addLeaderLock(TransactionLockTypes.LockType.WRITE);
                return tla.acquireLeaderLock();
            }

            @Override
            public Object performTask() throws IOException {
                return leaderElector.getActiveNamenodes().getActiveNamenodes();
            }
        }.handle();
    }

    private Long getMaxCounter(final LeaderElection leaderElector) throws IOException {
        return (Long) new HDFSTransactionalRequestHandler(HDFSOperationType.LEADER_ELECTION) {

            @Override
            public TransactionLocks acquireLock() throws PersistanceException, IOException {
                HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
                tla.getLocks().addLeaderTocken(TransactionLockTypes.LockType.WRITE);
                tla.getLocks().addLeaderLock(TransactionLockTypes.LockType.WRITE);
                return tla.acquireLeaderLock();
            }

            @Override
            public Object performTask() throws IOException {
                return leaderElector.getMaxNamenodeCounter();
            }
        }.handle();
    }

    private List<HopLeader> getAllNN(final LeaderElection leaderElector) throws IOException {
        return (List<HopLeader>) new HDFSTransactionalRequestHandler(HDFSOperationType.LEADER_ELECTION) {

            @Override
            public TransactionLocks acquireLock() throws PersistanceException, IOException {
                HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
                tla.getLocks().addLeaderTocken(TransactionLockTypes.LockType.WRITE);
                tla.getLocks().addLeaderLock(TransactionLockTypes.LockType.WRITE);
                return tla.acquireLeaderLock();
            }

            @Override
            public Object performTask() throws IOException {
                return leaderElector.getAllNameNodes();
            }
        }.handle();
    }
}
