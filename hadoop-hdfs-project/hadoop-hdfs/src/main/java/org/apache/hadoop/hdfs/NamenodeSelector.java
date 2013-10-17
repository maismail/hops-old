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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    /* List of name nodes */
    ConcurrentHashMap<InetSocketAddress, DFSClient> clients = new ConcurrentHashMap<InetSocketAddress, DFSClient>();
    private static Log LOG = LogFactory.getLog(NamenodeSelector.class);
    private final Statistics statistics;
    private final URI defaultUri;
    private final NNSelectionPolicy policy;
    private final Configuration conf;
    private boolean periodicNNListUpdate = true;
    private final Object wiatObjectForUpdate = new Object();

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
     NamenodeSelector(Configuration conf, URI defaultUri, Statistics statistics) throws IOException  {
        this.statistics = statistics;
        this.defaultUri = defaultUri;
        this.conf = conf;

        // Getting appropriate policy
        // supported policies are 'RANDOM' and 'ROUND_ROBIN'
        String policyName = conf.get(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, "ROUND_ROBIN");
        if (policyName == NNSelectionPolicy.RANDOM.toString()) {
            policy = NNSelectionPolicy.RANDOM;
        } else if (policyName == NNSelectionPolicy.ROUND_ROBIN.toString()) {
            policy = NNSelectionPolicy.ROUND_ROBIN;
        } else {
            policy = NNSelectionPolicy.ROUND_ROBIN;
        }
        LOG.debug("Namenode selection policy is set to " + policy);

        //get the list of Namenodes
        createDFSClientsFromList();
       
        //start periodic Namenode list update thread.
        start();
    }

    @Override
    public void run() {
        while (periodicNNListUpdate) {
            try {
                //first sleep and then update
                synchronized(wiatObjectForUpdate){
                wiatObjectForUpdate.wait(3000);
                }
                if (periodicNNListUpdate) {
                    periodicDFSClientsUpdate();
                }
            } catch (Exception ex) {
                LOG.warn(ex);
                ex.printStackTrace();
            }
        }
    }

    public synchronized void close() throws IOException{
        periodicNNListUpdate = false;
        //this.interrupt();  //close the periodic update loop
        
        //close all clients
        for(InetSocketAddress key : clients.keySet()){
            DFSClient client = clients.get(key);
            clients.remove(key);
            client.close();
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
    public DFSClient getNextNamenode() throws IOException {
        if (clients == null || clients.isEmpty()) {
            return null;
        }

        int index = 0;
        DFSClient client = null;
        int maxRetries = clients.size();
        while (client == null && maxRetries > 0) {
            index = getNNIndex();
            Iterator<InetSocketAddress> itr = clients.keySet().iterator();
            for(int i = 0; i < index; i++){
               if(i == index) break;
               itr.next();
            }
            client = clients.get(itr.next());

            //return a good NN.
            if (!client.pingNamenode()) {
                LOG.debug("NN [" + client.getId() + "] failed. Trying next NN...");
                client = null;
                maxRetries--;
                continue;
            } else {
                LOG.debug("Returning NN" +client.getId()+" for next RPC call. RRIndex "+index);
                printNamenodes();
                 return client;
            }
        }

        //notify the periodic update thread to update the list of namenodes
        wiatObjectForUpdate.notify();
        
        // At this point, we have tried almost all NNs, all are not reachable. Something is wrong
        throw new IOException("getNextNamenode() :: Unable to connect to any Namenode");
    }
    
    private int getNNIndex() {
        if (policy == NNSelectionPolicy.RANDOM) {
            Random rand = new Random();
            rand.setSeed(System.currentTimeMillis());
            return rand.nextInt(clients.size());
        } else if (policy == NNSelectionPolicy.ROUND_ROBIN) {
            rrIndex = (++rrIndex) % clients.size();
            return rrIndex;
        } else {
            throw new UnsupportedOperationException("Namenode selection policy is not supported. Selected policy is " + policy);
        }
    }

    void printNamenodes() {
        String nns = "Client is connected to namenodes: ";
        for (InetSocketAddress address : clients.keySet()) {
            DFSClient clinet = clients.get(address);
            nns += "[ Client ID "+clinet.getId()+", NN Addr: "+ address + "], ";
        }
        LOG.debug(nns);
    }

    public int getTotalNamenodes() {
        return clients.size();
    }

    /**
     * try connecting to the default uri and get the list of NN from there if it
     * fails then read the list of NNs from the config file and connect to them
     * for list of Namenodes in the syste.
     */
    private void createDFSClientsFromList() throws IOException {

        SortedActiveNamenodeList anl = null;

        if (defaultUri != null) {
            try {
                DFSClient defaultClient = new DFSClient(defaultUri, conf, statistics);
                anl = defaultClient.getActiveNamenodes();
                defaultClient.close();
            } catch (Exception e) {
                LOG.debug("Failed to get list of NN from default NN. Default NN was " + defaultUri);
            }

        }

        if (anl == null) { // default failed, now try the list of NNs from the config file
            String[] namenodes = getNamenodesFromConfigFile(conf);
            if (namenodes.length > 0) {
                for (int i = 0; i < namenodes.length; i++) {
                    DFSClient dfsClient = null;
                    try {
                        URI uri = new URI(namenodes[i]);
                        dfsClient = new DFSClient(uri, conf, statistics);
                        anl = dfsClient.getActiveNamenodes();
                        if (anl != null && !anl.getActiveNamenodes().isEmpty()) {
                            dfsClient.close();
                            break; // we got the list
                        }
                    } catch (Exception e) {
                        dfsClient.close();
                    }
                }
            }
        }
        refreshNamenodeList(anl);
    }

    /**
     * we already have a list of NNs in 'clinets' try contacting these namenodes
     * to get a fresh list of namenodes if all of the namenodes in the 'clients'
     * map fail then call the 'createDFSClientsForFirstTime' function. with will
     * try to connect to defaults namenode provided at the initialization phase.
     */
    private void periodicDFSClientsUpdate() throws IOException {
        SortedActiveNamenodeList anl = null;
        if (!clients.isEmpty()) {
            for (DFSClient clinet : clients.values()) {
                try {
                    anl = clinet.getActiveNamenodes();
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
            createDFSClientsFromList();
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
        //NOTE should not restart a valid client

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
                removeDFSClient(deadNNAddr);
            }
        }

        // start threads for new NNs
        if (newNNs.size() != 0) {
            for (InetSocketAddress newNNAddr : newNNs) {
                addDFSClient(newNNAddr);
            }
        }
        
        printNamenodes();
    }
    
    //thse are synchronized using external methods
    private void removeDFSClient(InetSocketAddress address){
        try{
        DFSClient deadClient = clients.get(address);
        clients.remove(address);
        deadClient.close();
        LOG.debug("TestNN removed DFSClient for " + address);
        }catch(IOException e){
            LOG.debug("Unable to Stop DFSClient for "+ address);
        }
    }
    //thse are synchronized using external methods
    private void addDFSClient(InetSocketAddress address){
        try
        {
            URI uri = NameNode.getUri(address);
            DFSClient newClient = new DFSClient(uri, conf, statistics);
            clients.put(address, newClient);
        }
        catch(IOException e){
           LOG.debug("Unable to Start DFSClient for "+ address); 
        }
    }
}
