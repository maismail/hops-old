package org.apache.hadoop.hdfs;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import static org.apache.hadoop.hdfs.NamenodeSelector.rrIndex;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import org.apache.hadoop.hdfs.server.protocol.SortedActiveNamenodeList;
import org.apache.hadoop.ipc.RPC;

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
public class NamenodeSelector extends Thread {

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

    class NamenodeHandle {

        final private ClientProtocol namenodeRPCHandle;
        final private ActiveNamenode namenode;

        NamenodeHandle(ClientProtocol proto, ActiveNamenode an) {
            this.namenode = an;
            this.namenodeRPCHandle = proto;
        }

        public ClientProtocol getRPCHandle() {
            return this.namenodeRPCHandle;
        }

        public ActiveNamenode getNamenode() {
            return this.namenode;
        }

        @Override
        public String toString() {
            return "[RPC handle connected to " + namenode.getInetSocketAddress() + "] ";
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof NamenodeSelector.NamenodeHandle)) {
                return false;
            } else {
                NamenodeSelector.NamenodeHandle that = (NamenodeSelector.NamenodeHandle) obj;
                boolean res = this.namenode.equals(that.getNamenode());
                LOG.debug("Equals returning " + res + " for this " + namenode + " that " + that.getNamenode());
                return res;
            }
        }
    };
    /* List of name nodes */
    private List<NamenodeSelector.NamenodeHandle> nnList = new CopyOnWriteArrayList<NamenodeSelector.NamenodeHandle>();
    private List<NamenodeSelector.NamenodeHandle> blackListedNamenodes = new CopyOnWriteArrayList<NamenodeSelector.NamenodeHandle>();
    private static Log LOG = LogFactory.getLog(NamenodeSelector.class);
    private final URI defaultUri;
    private final NamenodeSelector.NNSelectionPolicy policy;
    private final Configuration conf;
    private boolean periodicNNListUpdate = true;
    private final Object wiatObjectForUpdate = new Object();
    private final int namenodeListUpdateTimePeriod;

    NamenodeSelector(Configuration conf, ClientProtocol namenode) throws IOException {
        this.defaultUri = null;
        this.nnList.add(new NamenodeSelector.NamenodeHandle(namenode, null));
        this.conf = conf;
        this.policy = NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN;
        this.namenodeListUpdateTimePeriod = -1;
    }

    NamenodeSelector(Configuration conf, URI defaultUri) throws IOException {
        this.defaultUri = defaultUri;
        this.conf = conf;
        namenodeListUpdateTimePeriod = conf.getInt(DFSConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST, DFSConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_DEFAULT);

        // Getting appropriate policy
        // supported policies are 'RANDOM' and 'ROUND_ROBIN'
        String policyName = conf.get(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, "ROUND_ROBIN");
        if (policyName == NamenodeSelector.NNSelectionPolicy.RANDOM.toString()) {
            policy = NamenodeSelector.NNSelectionPolicy.RANDOM;
        } else if (policyName == NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN.toString()) {
            policy = NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN;
        } else {
            policy = NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN;
        }
        LOG.debug("Namenode selection policy is set to " + policy);

        //get the list of Namenodes
        createNamenodeClinetsFromList();

        //start periodic Namenode list update thread.
        start();
    }

    @Override
    public void run() {
        while (periodicNNListUpdate) {
            try {
                //first sleep and then update
                synchronized (wiatObjectForUpdate) {
                    wiatObjectForUpdate.wait(namenodeListUpdateTimePeriod);
                }
                if (periodicNNListUpdate) {
                    periodicNamenodeClientsUpdate();
                }
            } catch (Exception ex) {
                LOG.warn(ex);
                ex.printStackTrace();
            }
        }
    }

    public synchronized void close() throws IOException {
        stopPeriodicUpdates();

        //close all clients
        for (NamenodeSelector.NamenodeHandle namenode : nnList) {
            ClientProtocol rpc = namenode.getRPCHandle();
            nnList.remove(namenode);
            RPC.stopProxy(rpc);
        }
    }

    public void stopPeriodicUpdates() {
        periodicNNListUpdate = false;
    }
    /**
     * Gets the appropriate namenode for a read/write operation
     *
     * @return DFSClient
     */
    static int rrIndex = 0;

    public NamenodeSelector.NamenodeHandle getNextNamenode(AtomicLong txid) throws IOException {
        if (nnList == null || nnList.isEmpty()) {
            return null;
        }

        int index = 0;
        ClientProtocol client = null;
        int maxRetries = nnList.size();

        NamenodeSelector.NamenodeHandle handle = getNextNNBasedOnPolicy();
        if (handle == null) {
            //notify the periodic update thread to update the list of namenodes
            wiatObjectForUpdate.notify();

            // At this point, we have tried almost all NNs, all are not reachable. Something is wrong
            throw new IOException("getNextNamenode() :: Unable to connect to any Namenode");
        }
        client = handle.getRPCHandle();
        LOG.debug(txid + ") Returning " + handle + " for next RPC call. RRIndex " + index);
        LOG.debug(txid + ") " + printNamenodes());
        return handle;



    }

    private NamenodeSelector.NamenodeHandle getNextNNBasedOnPolicy() {
        if (policy == NamenodeSelector.NNSelectionPolicy.RANDOM) {
            for (int i = 0; i < 10; i++) {
                Random rand = new Random();
                rand.setSeed(System.currentTimeMillis());
                int index = rand.nextInt(nnList.size());
                NamenodeSelector.NamenodeHandle handle = nnList.get(index);
                if (!this.blackListedNamenodes.contains(handle)) {
                    return handle;
                }
            }
            return null;
        } else if (policy == NamenodeSelector.NNSelectionPolicy.ROUND_ROBIN) {
            for (int i = 0; i < 10; i++) {
                rrIndex = (++rrIndex) % nnList.size();
                NamenodeSelector.NamenodeHandle handle = nnList.get(rrIndex);
                if (!this.blackListedNamenodes.contains(handle)) {
                    LOG.debug("returning idex for " + handle);
                    return handle;
                }
            }
            return null;
        } else {
            throw new UnsupportedOperationException("Namenode selection policy is not supported. Selected policy is " + policy);
        }
    }

    String printNamenodes() {
        String nns = "Client is connected to namenodes: ";
        for (NamenodeSelector.NamenodeHandle namenode : nnList) {
            nns += namenode + ", ";
        }

        nns += " Black Listed Nodes are ";
        for (NamenodeSelector.NamenodeHandle namenode : this.blackListedNamenodes) {
            nns += namenode + ", ";
        }
        return nns;
    }

    public int getTotalConnectedNamenodes() {
        return nnList.size();
    }

    /**
     * try connecting to the default uri and get the list of NN from there if it
     * fails then read the list of NNs from the config file and connect to them
     * for list of Namenodes in the syste.
     */
    private void createNamenodeClinetsFromList() throws IOException {
        SortedActiveNamenodeList anl = null;
        ClientProtocol handle = null;
        if (defaultUri != null) {
            try {
                NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = NameNodeProxies.createProxy(conf, defaultUri, ClientProtocol.class);
                handle = proxyInfo.getProxy();
                if (handle != null) {
                    anl = handle.getActiveNamenodesForClient();
                    RPC.stopProxy(handle);
                }
            } catch (Exception e) {
                LOG.debug("Failed to get list of NN from default NN. Default NN was " + defaultUri);
                RPC.stopProxy(handle);
            }
        }

        if (anl == null) { // default failed, now try the list of NNs from the config file
            String[] namenodes = getNamenodesFromConfigFile(conf);
            if (namenodes.length > 0) {
                for (int i = 0; i < namenodes.length; i++) {
                    try {
                        URI uri = new URI(namenodes[i]);
                        NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = NameNodeProxies.createProxy(conf, uri, ClientProtocol.class);
                        handle = proxyInfo.getProxy();
                        if (handle != null) {
                            anl = handle.getActiveNamenodesForClient();
                            RPC.stopProxy(handle);
                        }
                        if (anl != null && !anl.getActiveNamenodes().isEmpty()) {
                            break; // we got the list
                        }
                    } catch (Exception e) {
                        RPC.stopProxy(handle);
                    }
                }
            }
        }
        if (anl != null) {
            refreshNamenodeList(anl);
        }
    }

    /**
     * we already have a list of NNs in 'clinets' try contacting these namenodes
     * to get a fresh list of namenodes if all of the namenodes in the 'clients'
     * map fail then call the 'createDFSClientsForFirstTime' function. with will
     * try to connect to defaults namenode provided at the initialization phase.
     */
    private void periodicNamenodeClientsUpdate() throws IOException {
        SortedActiveNamenodeList anl = null;
        if (!nnList.isEmpty()) {
            for (NamenodeSelector.NamenodeHandle namenode : nnList) { //TODO dont try with black listed nodes
                try {
                    ClientProtocol handle = namenode.getRPCHandle();
                    anl = handle.getActiveNamenodesForClient();
                    if (anl == null || anl.size() == 0) {
                        anl = null;
                        continue;
                    } else {
                        // we got a fresh list of anl
                        refreshNamenodeList(anl);
                        return;
                    }
                } catch (IOException e) {
                    continue;
                }
            }
        }

        if (anl == null) { // try contacting default NNs
            createNamenodeClinetsFromList();
        }
    }

    private String[] getNamenodesFromConfigFile(Configuration conf) {
        String namenodes = conf.get(DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_KEY);
        if (namenodes == null || namenodes.length() == 0) {
            return new String[0];
        }
        return namenodes.split(",");
    }

    private synchronized void refreshNamenodeList(SortedActiveNamenodeList anl) {
        if (anl == null) {
            return;
        }
        LOG.debug("refreshing namenodes new anl is size " + anl.size());
        //NOTE should not restart a valid client

        //find out which client to start and stop  
        //make sets objects of old and new lists
        Set<InetSocketAddress> oldClients = Sets.newHashSet();
        for (NamenodeSelector.NamenodeHandle namenode : nnList) {
            ActiveNamenode ann = namenode.getNamenode();
            oldClients.add(ann.getInetSocketAddress());
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
                removeDFSClient(deadNNAddr);
            }
        }

        // start threads for new NNs
        if (newNNs.size() != 0) {
            for (InetSocketAddress newNNAddr : newNNs) {
                addDFSClient(newNNAddr, anl.getActiveNamenode(newNNAddr));
            }
        }

        //clear black listed nodes
        this.blackListedNamenodes.clear();

        LOG.debug(printNamenodes());
    }

    //thse are synchronized using external methods
    private void removeDFSClient(InetSocketAddress address) {
        NamenodeSelector.NamenodeHandle handle = getNamenodeHandle(address);
        if (handle != null) {
            if (nnList.remove(handle)) {
                RPC.stopProxy(handle.getRPCHandle());
                LOG.debug("Removed RPC proxy for " + address);
            } else {
                LOG.debug("Failed to Remove RPC proxy for " + address);
            }
        }
    }
    //thse are synchronized using external methods

    private void addDFSClient(InetSocketAddress address, ActiveNamenode ann) {
        if (address == null || ann == null) {
            LOG.warn("Unable to add proxy for namenode. ");
            return;
        }
        try {
            URI uri = NameNode.getUri(address);
            NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = NameNodeProxies.createProxy(conf, uri, ClientProtocol.class);
            ClientProtocol handle = proxyInfo.getProxy();
            nnList.add(new NamenodeSelector.NamenodeHandle(handle, ann));
        } catch (IOException e) {
            LOG.debug("Unable to Start RPC proxy for " + address);
        }
    }

    private boolean pingNamenode(ClientProtocol namenode) {
        try {
            namenode.ping();
            return true;
        } catch (IOException ex) {
            return false;
        }
    }

    private NamenodeSelector.NamenodeHandle getNamenodeHandle(InetSocketAddress address) {
        for (NamenodeSelector.NamenodeHandle handle : nnList) {
            if (handle.getNamenode().getInetSocketAddress().equals(address)) {
                return handle;
            }
        }
        return null;
    }

    public void blackListNamenode(NamenodeSelector.NamenodeHandle handle) {
        if (!this.blackListedNamenodes.contains(handle)) {
            this.blackListedNamenodes.add(handle);
        }


        //if a bad namenode is detected then update the list of Namenodes in the system
        synchronized (wiatObjectForUpdate) {
            wiatObjectForUpdate.notify();
        }
    }
}
