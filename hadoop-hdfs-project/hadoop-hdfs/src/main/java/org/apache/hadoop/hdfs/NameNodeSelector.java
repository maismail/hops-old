package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.SortedActiveNamenodeList;

/****************************************************************
This interface is a policy for selecting the appropriate NameNodes 
 * (either in round robin or random style or any other style depending on implementation)
 * to perform hdfs read/write operations
 * 
 * In case we are not able to load the class provided in the configuration, we load default policy (which is Random selection)
 *
 *****************************************************************/
public abstract class NameNodeSelector {
  /* List of name nodes */
  protected List<DFSClient> namenodes;
  /* Namenodes that did not respond to ping messages. Hence might have crashed */
  private List<DFSClient> inactiveNamenodes = new ArrayList<DFSClient>();
  private static Log LOG = LogFactory.getLog(NameNodeSelector.class);

  /** Loads the appropriate instance of the the NameNodeSelector from the configuration file
   * So that appropriate reader/ writer namenodes can be selected for each operation
   * @param conf - The configuration from hdfs
   * @param namenodes - The list of namenodes for read/write operations
   * @return NameNodeSelector - Returns the implementor of the NameNodeSelector depending on the policy as specified the configuration
   */
  public static NameNodeSelector createInstance(Configuration conf, List<DFSClient> nns) {

    NameNodeSelector nnSelectorPolicy = null;

    boolean error = false;

    // Getting appropriate policy
    String className = conf.get(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, RoundRobinNameNodeSelector.class.getName());

    Object objSelector = null;

    try {
      Class clsSelector = Class.forName(className);
      objSelector = clsSelector.newInstance();

      if (objSelector instanceof NameNodeSelector) {

        // Setting the 'namenodes' field at runtime
        Field field = clsSelector.getSuperclass().getDeclaredField("namenodes");
        field.set(objSelector, nns);

        nnSelectorPolicy = (NameNodeSelector) objSelector;

      }
      else {
        LOG.warn("getInstance() :: Invalid class type provided for name node selector policy. Not an instance of abstract class NameNodeSelector [class-name: " + className + "]");
        error = true;
      }
    }
    catch (Exception ex) {
      LOG.warn("Exception n NameNodeSelector [" + className + "]", ex);
      error = true;
    }
    
    if (error) { //Default
      // In case of error, get default name node selector policy
      LOG.info("Selecting default Namenode selection policy");
      RoundRobinNameNodeSelector policy = new RoundRobinNameNodeSelector();
      policy.namenodes = nns;
      return policy;
    }
    else {
      // No errors
      LOG.info("Successfully loaded Namenode selector policy [" + nnSelectorPolicy.getClass().getName() + "]");
      return nnSelectorPolicy;
    }
  }

  /**Gets the appropriate namenode for a read/write operation
   * @return DFSClient
   */
  protected abstract DFSClient getNamenode();

  /**Gets the appropriate writer namenode for a read/write operation by policy and retries for next namenode incase of failure
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
      }
      else {
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
      }
      else {
        return false;
      }
    }
    else {
      return false;
    }
  }
  
  
  private void createDfsClients(URI defaultUri, Configuration conf, Statistics statistics) throws IOException {
    
    SortedActiveNamenodeList anl = null;
    //try connecting to the default uri and get the list of NN from there
    //if it fails then read the list of NNs from the config file and connect to 
    //them for list of Namenodes in the syste. 
    if(defaultUri != null){
      DFSClient defaultClient = new DFSClient(defaultUri, conf, statistics);
      try{
        anl = defaultClient.getActiveNamenodes();
      }catch(Exception e){
        LOG.debug("Failed to get list of NN from default NN. Default NN was "+defaultUri);
      }
      defaultClient.close();
    }
    
    if(anl == null){ // default failed, now try the list of NNs from the config file
      String[] namenodes = getNamenodes(conf);
      if (namenodes.length > 0) {
          for (int i = 0; i < namenodes.length; i++) {
            DFSClient dfsClient = null;
            try{
            URI uri = new URI(namenodes[i]);
            dfsClient = new DFSClient(uri, conf, statistics);
            anl = dfsClient.getActiveNamenodes();
            if(anl != null && !anl.getActiveNamenodes().isEmpty()){
              break; // we got the list
            }
            }catch(Exception e){}
            finally{
              dfsClient.close();
            }
          }
      }
    }
    refreshNamenodeList(anl);
  }
  
  private String[] getNamenodes(Configuration conf) {
    String namenodes = conf.get(DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_KEY);
    if(namenodes == null || namenodes.length() == 0) {
      return new String[0];
    }
    return namenodes.split(",");
  }
  List<DFSClient> clients = new ArrayList();
  private void refreshNamenodeList(SortedActiveNamenodeList anl)
  {
    //find out which client to start and stop
    
  }
}
