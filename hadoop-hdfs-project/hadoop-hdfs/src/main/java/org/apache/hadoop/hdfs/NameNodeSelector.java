package org.apache.hadoop.hdfs;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import org.apache.hadoop.hdfs.server.protocol.SortedActiveNamenodeList;

/**
 * **************************************************************
 * This interface is a policy for selecting the appropriate NameNodes (either in
 * round robin or random style or any other style depending on implementation)
 * to perform hdfs read/write operations
 *
 * In case we are not able to load the class provided in the configuration, we
 * load default policy (which is Random selection)
 *
 ****************************************************************
 */
public abstract class NameNodeSelector {

    enum NNSelectionPolicy {

        RANDOM("RANDOM"),
        ROUND_ROBIN("ROUND_ROBIN");
        private String description = null;

        private NNSelectionPolicy(String arg) {
            this.description = arg;
        }

        @Override
        public String toString() {
            return description;
        }
    }
    /* List of name nodes */
    protected List<DFSClient> namenodes;
    /* Namenodes that did not respond to ping messages. Hence might have crashed */
    private List<DFSClient> inactiveNamenodes = new ArrayList<DFSClient>();
    private static Log LOG = LogFactory.getLog(NameNodeSelector.class);
    private final Statistics statistics;
    private final URI defaultUri;
    private final NNSelectionPolicy policy;

    /**
     * Loads the appropriate instance of the the NameNodeSelector from the
     * configuration file So that appropriate reader/ writer namenodes can be
     * selected for each operation
     *
     * @param conf - The configuration from hdfs
     * @param namenodes - The list of namenodes for read/write operations
     * @return NameNodeSelector - Returns the implementor of the
     * NameNodeSelector depending on the policy as specified the configuration
     */
    public NameNodeSelector(Configuration conf, URI defaultUri, Statistics statistics) {
        this.statistics = statistics;
        this.defaultUri = defaultUri;

        boolean error = false;

        // Getting appropriate policy
        // supported policies are 'RANDOM' and 'ROUND_ROBIN'
        String policyName = conf.get(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, "ROUND_ROBIN");
        if(policyName == NNSelectionPolicy.RANDOM.toString()){
            policy = NNSelectionPolicy.RANDOM;
        }else if(policyName == NNSelectionPolicy.ROUND_ROBIN.toString()){
            policy = NNSelectionPolicy.ROUND_ROBIN;
        }else{
            policy = NNSelectionPolicy.ROUND_ROBIN;
        }
        LOG.debug("Namenode selection policy is set to "+policy);
    }
    /**
     * Gets the appropriate namenode for a read/write operation
     *
     * @return DFSClient
     */
    static int rrIndex = 0;
    private int getNamenodeId() {
        if(policy == NNSelectionPolicy.RANDOM){
            Random rand = new Random();
            rand.setSeed(System.currentTimeMillis());
            return rand.nextInt(clients.size());
        }
        else if(policy == NNSelectionPolicy.ROUND_ROBIN){
            rrIndex = (rrIndex++)/clients.size();
            return rrIndex;
        }
        else{
            throw new UnsupportedOperationException("Namenode selection policy is not supported. Selected policy is "+policy);
        }
    }

    /**
     * Gets the appropriate writer namenode for a read/write operation by policy
     * and retries for next namenode incase of failure
     *
     * @return DFSClient
     */
    public DFSClient getNextNamenode() throws IOException {

        for (int nnIndex = 1; nnIndex <= namenodes.size(); nnIndex++) {

            // Returns next nn based on policy
            DFSClient client = getNamenode();
            LOG.info("Next NN: " + client.getId());

            // skip over inactive namenodes
            if (inactiveNamenodes.contains(client)) {
                continue;
            }

            // check for connectivity with namenode
            if (client.pingNamenode()) {
                return client;
            } else {
                inactiveNamenodes.add(client);
                LOG.warn("NN [" + client.getId() + "] failed. Trying next NN...");
            }
            // Switch to next Writer nn
        }

        // At this point, we have tried almost all NNs, all are not reachable. Something is wrong
        throw new IOException("getNextNamenode() :: Unable to connect to any Namenode");
    }

    void printNamenodes() {
        String nns = "namenodes: ";
        for (DFSClient client : namenodes) {
            nns += client.getId() + ", ";
        }
        LOG.debug(nns);
    }

    public int getTotalNamenodes() {
        return namenodes.size();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NameNodeSelector) {
            // [JUDE] Done just for testing
            if ((((NameNodeSelector) o).namenodes.size() == this.namenodes.size())) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private void createDfsClients(URI defaultUri, Configuration conf) throws IOException {

        SortedActiveNamenodeList anl = null;
        //try connecting to the default uri and get the list of NN from there
        //if it fails then read the list of NNs from the config file and connect to 
        //them for list of Namenodes in the syste. 
        if (defaultUri != null) {
            DFSClient defaultClient = new DFSClient(defaultUri, conf, statistics);
            try {
                anl = defaultClient.getActiveNamenodes();
            } catch (Exception e) {
                LOG.debug("Failed to get list of NN from default NN. Default NN was " + defaultUri);
            }
            defaultClient.close();
        }

        if (anl == null) { // default failed, now try the list of NNs from the config file
            String[] namenodes = getNamenodes(conf);
            if (namenodes.length > 0) {
                for (int i = 0; i < namenodes.length; i++) {
                    DFSClient dfsClient = null;
                    try {
                        URI uri = new URI(namenodes[i]);
                        dfsClient = new DFSClient(uri, conf, statistics);
                        anl = dfsClient.getActiveNamenodes();
                        if (anl != null && !anl.getActiveNamenodes().isEmpty()) {
                            break; // we got the list
                        }
                    } catch (Exception e) {
                    } finally {
                        dfsClient.close();
                    }
                }
            }
        }
        refreshNamenodeList(anl);
    }

    private String[] getNamenodes(Configuration conf) {
        String namenodes = conf.get(DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_KEY);
        if (namenodes == null || namenodes.length() == 0) {
            return new String[0];
        }
        return namenodes.split(",");
    }
    HashMap<InetSocketAddress, DFSClient> clients = new HashMap<InetSocketAddress, DFSClient>();

    private void refreshNamenodeList(SortedActiveNamenodeList anl) {
        //find out which client to start and stop  
        //make sets objects of old and new lists
        Set<InetSocketAddress> oldClients = Sets.newHashSet();
        for (InetSocketAddress nnAddress : clients.keySet()) {
            oldClients.add(nnAddress);
        }

        //new list
        Set<InetSocketAddress> updatedClients = Sets.newHashSet();
        for (ActiveNamenode ann : anl.getActiveNamenodes()) {
            updatedClients.add(ann.getInetSocketAddress());
        }

        //START_HOP_CODE
        Sets.SetView<InetSocketAddress> deadNNs = Sets.difference(oldClients, updatedClients);
        Sets.SetView<InetSocketAddress> newNNs = Sets.difference(updatedClients, oldClients);

        // stop the dead threads 
        if (deadNNs.size() != 0) {
            for (InetSocketAddress deadNNAddr : deadNNs) {
                DFSClient deadClient = clients.get(deadNNAddr);
                clients.remove(deadNNAddr);
                deadClient.close();
                LOG.debug("TestNN stopped DFSClient for " + deadNNAddr);
            }
        }

        // start threads for new NNs
        if (newNNs.size() != 0) {
            for (InetSocketAddress newNNAddr : newNNs) {
                DFSClient newClient = new Statts
        
                LOG.debug("TestNN started DFSClient for " + newNNAddr);
            }
        }

    }
}
