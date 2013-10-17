package org.apache.hadoop.hdfs.server.namenode;

import java.io.EOFException;
import java.io.IOException;
import static java.lang.Thread.sleep;
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;

import org.apache.log4j.Level;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 *
 * @author Salman
 */
public class TestHABasicFileCreation extends junit.framework.TestCase {

    public static final Log LOG = LogFactory.getLog(TestHABasicFileCreation.class);

    {
        ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    }
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    int NN1 = 0, NN2 = 1;
    static int NUM_NAMENODES = 2;
    static int NUM_DATANODES = 1;
    // 10 seconds timeout default
    long timeout = 10000;
    Path dir = new Path("/testsLoad");
    //Writer[] writers = new Writer[10];
    Writer[] writers = new Writer[1];

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    private void setupCluster(int replicationFactor) throws IOException {
        // initialize the cluster with minimum 2 namenodes and minimum 6 datanodes
        if (NUM_NAMENODES < 2) {
            NUM_NAMENODES = 2;
        }
        
        if(replicationFactor > NUM_DATANODES){
            NUM_DATANODES = replicationFactor;
        }

        this.conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
        conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
        //conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10 * 1000); // 10 sec
        //conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 3);  // 3 sec

        cluster = new MiniDFSCluster.Builder(conf).nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).numDataNodes(NUM_DATANODES).build();
        cluster.waitActive();
        
        fs = cluster.getNewFileSystemInstance(NN1);

        timeout = conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_KEY, DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_DEFAULT)
                + conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;


        // create the directory namespace
        assertTrue(fs.mkdirs(dir));

        // create writers
        for (int i = 0; i < writers.length; i++) {
            writers[i] = new Writer(fs, new Path(dir, "file" + i));
        }


    }

    private void shutdown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    private void startWriters() {
        for (int i = 0; i < writers.length; i++) {
            writers[i].start();
        }
    }

    private void stopWriters() throws InterruptedException {
        for (int i = 0; i < writers.length; i++) {
            if (writers[i] != null) {
                writers[i].running = false;
                writers[i].interrupt();
            }
        }
        for (int i = 0; i < writers.length; i++) {
            if (writers[i] != null) {
                writers[i].join();
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    private void verifyFile() throws IOException {
        LOG.info("Verify the file");
        for (int i = 0; i < writers.length; i++) {
            LOG.info(writers[i].filepath + ": length=" + fs.getFileStatus(writers[i].filepath).getLen());
            FSDataInputStream in = null;
            try {
                in = fs.open(writers[i].filepath);
                boolean eof = false;
                int j = 0, x = 0;
                long dataRead = 0;
                while (!eof) {
                    try {
                        x = in.readInt();
                        dataRead++;
                        assertEquals(j, x);
                        j++;
                    } catch (EOFException ex) {
                        eof = true; // finished reading file
                    }
                }
                if(writers[i].datawrote != dataRead){
                    fail("File length read lenght is not consistant. wrote "+writers[i].datawrote+" data read "+dataRead);
                }
            } finally {
                IOUtils.closeStream(in);
            }
        }
    }


    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /**
     * Under load perform failover by killing leader NN1 NN2 will be active and
     * loads are now processed by NN2 Load should still continue No corrupt
     * blocks should be reported
     */
    @Test
    public void testFailoverWhenLeaderNNCrashes() {
        // Testing with replication factor of 3
        short repFactor = 1;
        LOG.info("Running test [testFailoverWhenLeaderNNCrashes()] with replication factor " + repFactor);
        failoverWhenLeaderNNCrashes(repFactor);
        // Testing with replication factor of 6
//    repFactor = 6;
//    LOG.info("Running test [testFailoverWhenLeaderNNCrashes()] with replication factor " + repFactor);
//    failoverWhenLeaderNNCrashes(repFactor);
    }

    private void failoverWhenLeaderNNCrashes(short replicationFactor) {
        try {
            // setup the cluster with required replication factor
            setupCluster(replicationFactor);

            // save leader namenode port to restart with the same port
            int nnport = cluster.getNameNodePort(NN1);

            try {
                // writers start writing to their files
                startWriters();

                // Give all the threads a chance to create their files and write something to it
                Thread.sleep(5000); // 50 sec

                // kill leader NN1
                cluster.shutdownNameNode(NN1);
                TestHABasicFailover.waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2), timeout);
                // Check NN2 is the leader and failover is detected
                assertTrue("NN2 is expected to be the leader, but is not", cluster.getNameNode(NN2).isLeader());
                assertTrue("Not all datanodes detected the new leader", TestHABasicFailover.doesDataNodesRecognizeLeader(cluster.getDataNodes(), cluster.getNameNode(NN2)));

                // the load should still continue without any IO Exception thrown
                LOG.info("Wait a few seconds. Let them write some more");
                Thread.sleep(5000);

            } finally {
                stopWriters();
            }
            
            verifyFile();
            // the block report intervals would inform the namenode of under replicated blocks
            // hflush() and close() would guarantee replication at all datanodes. This is a confirmation
            waitReplication(fs, dir, replicationFactor, timeout);

            if (true) {
                return;
            }
            // restart the cluster without formatting using same ports and same configurations
            cluster.shutdown();
            cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnport).format(false).nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).numDataNodes(NUM_DATANODES).build();
            cluster.waitActive();

            // update the client so that it has the fresh list of namenodes. Black listed namenodes will be removed
            fs = cluster.getNewFileSystemInstance(NN1);

            verifyFile(); // throws IOException. Should be caught by parent
        } catch (Exception ex) {
            LOG.error("Received exception: " + ex.getMessage(), ex);
            ex.printStackTrace();
            fail("Exception: " + ex.getMessage());
        } finally {
            shutdown();
        }

    }  

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void waitReplication(FileSystem fs, Path rootDir, short replicationFactor, long timeout) throws IOException, TimeoutException {
        FileStatus[] files = fs.listStatus(rootDir);
        for (int i = 0; i < files.length;) {
            try {
                // increasing timeout to take into consideration 'ping' time with failed namenodes
                // if the client fetches for block locations from a dead NN, it would need to retry many times and eventually this time would cause a timeout
                // to avoid this, we set a larger timeout
                long expectedRetyTime = 20000; // 20seconds
                timeout = timeout + expectedRetyTime;
                DFSTestUtil.waitReplicationWithTimeout(fs, files[i].getPath(), replicationFactor, timeout);
                i++;
            } catch (ConnectException ex) {
                LOG.warn("Received Connect Exception (expected due to failure of NN)");
                ex.printStackTrace();
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    static class Writer extends Thread {

        FileSystem fs;
        final Path filepath;
        boolean running = true;
        FSDataOutputStream outputStream = null;
        long datawrote = 0;

        Writer(FileSystem fs, Path filepath) {
            super(Writer.class.getSimpleName() + ":" + filepath);
            this.fs = fs;
            this.filepath = filepath;

            // creating the file here
            try {
                outputStream = this.fs.create(filepath);
            } catch (Exception ex) {
                LOG.info(getName() + " unable to create file [" + filepath + "]" + ex, ex);
                if (outputStream != null) {
                    IOUtils.closeStream(outputStream);
                    outputStream = null;
                }
            }
        }

        public void run() {

            int i = 0;
            if (outputStream != null) {
                try {
                    for (; running; i++) {
                        outputStream.writeInt(i);
                        datawrote++;
                        outputStream.hflush();
                        sleep(100);
                    }
                } catch (Exception e) {
                    LOG.info(getName() + " dies: e=" + e, e);
                } finally {
                    LOG.info(getName() + ": i=" + i);
                    IOUtils.closeStream(outputStream);
                }//end-finally
            }// end-outcheck
            else {
                LOG.info(getName() + " outstream was null for file  [" + filepath + "]");
            }
        }//end-run        
    }//end-method
}