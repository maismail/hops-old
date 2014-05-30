package se.sics.hop.metadata.lock;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import se.sics.hop.Common;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.RequestHandler;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.hdfs.dal.BlockLookUpDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopBlockLookUp;
import se.sics.hop.transaction.handler.HDFSOperationType;

/**
 *
 * @author hooman
 */
public class INodeUtil {

  private final static Log LOG = LogFactory.getLog(INodeUtil.class);

  // This code is based on FSDirectory code for resolving the path.
  //resolveLink indicates whether UnresolvedLinkException should
  public static boolean getNextChild(
          INode[] curInode,
          byte[][] components,
          int[] count,
          LinkedList<INode> resolvedInodes,
          boolean resolveLink,
          boolean transactional) throws UnresolvedPathException, PersistanceException {

    boolean lastComp = (count[0] == components.length - 1);
    if (curInode[0].isSymlink() && (!lastComp || (lastComp && resolveLink))) {
      final String symPath = constructPath(components, 0, components.length);
      final String preceding = constructPath(components, 0, count[0]);
      final String remainder =
              constructPath(components, count[0] + 1, components.length);
      final String link = DFSUtil.bytes2String(components[count[0]]);
      final String target = ((INodeSymlink) curInode[0]).getLinkValue();
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("UnresolvedPathException "
                + " path: " + symPath + " preceding: " + preceding
                + " count: " + count + " link: " + link + " target: " + target
                + " remainder: " + remainder);
      }
      throw new UnresolvedPathException(symPath, preceding, remainder, target);
    }

    if (lastComp || !curInode[0].isDirectory()) {
      return true;
    }

    curInode[0] = getNode(
            components[count[0] + 1],
            curInode[0].getId(),
            transactional);
    if (curInode[0] != null) {
      resolvedInodes.add(curInode[0]);
    }
    count[0] = count[0] + 1;
    lastComp = (count[0] == components.length - 1);
    return lastComp;
  }

  public static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  private static INode getNode(
          byte[] name,
          int parentId,
          boolean transactional)
          throws PersistanceException {
    String nameString = DFSUtil.bytes2String(name);
    if (transactional) {
      // TODO - Memcache success check - do primary key instead.
      LOG.debug("about to acquire lock on " + DFSUtil.bytes2String(name));
      return EntityManager.find(INode.Finder.ByPK_NameAndParentId, nameString, parentId);
    } else {
      return findINodeWithNoTransaction(nameString, parentId);
    }
  }

  private static INode findINodeWithNoTransaction(
          String name,
          int parentId)
          throws StorageException {
    LOG.info(String.format(
            "Read inode with no transaction by parent-id=%d, name=%s",
            parentId,
            name));
    INodeDataAccess<INode> da = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
    return da.pkLookUpFindInodeByNameAndParentId(name, parentId);
  }

  public static void resolvePathWithNoTransaction(
          String path,
          boolean resolveLink,
          LinkedList<INode> preTxResolvedINodes,
          boolean[] isPathFullyResolved
          )
          throws UnresolvedPathException, PersistanceException {
    preTxResolvedINodes.clear();
    isPathFullyResolved[0]  = false;

    if (path == null) {
      isPathFullyResolved[0] = false;
    }

    byte[][] components = INode.getPathComponents(path);
    INode[] curNode = new INode[1];

    int[] count = new int[]{0};
    boolean lastComp = (count[0] == components.length - 1);
    if (lastComp) // if root is the last directory, we should acquire the write lock over the root
    {
      preTxResolvedINodes.add(getRoot());
      isPathFullyResolved[0] = true;
    } else {
      curNode[0] = getRoot();
      preTxResolvedINodes.add(curNode[0]);
    }

    while (count[0] < components.length && curNode[0] != null) {

      lastComp = INodeUtil.getNextChild(
              curNode,
              components,
              count,
              preTxResolvedINodes,
              resolveLink,
              false);
      if (lastComp) {
        break;
      }
    }
      if (preTxResolvedINodes.size() != components.length) {
          isPathFullyResolved[0] = false;
      } else {
          isPathFullyResolved[0] = true;
      }
  }

//  public static INode findINodeByBlockId(final long blockId, final int partKey) throws StorageException {
//    LOG.debug(String.format(
//            "About to read block with no transaction by bid=%d",
//            blockId));
//      LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.TEST) {
//          @Override
//          public Object performTask() throws PersistanceException, IOException {
//              BlockInfoDataAccess<BlockInfo> bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
//              BlockInfo bInfo = bda.findById(blockId, partKey);
//              
//              if(bInfo == null) return null;
//              
//              INodeDataAccess<INode> ida = (INodeDataAccess)StorageFactory.getDataAccess(INodeDataAccess.class);
//              return ida.pruneIndexScanfindInodeById(bInfo.getInodeId(),partKey);
//          }
//      };
//    INode inode;
//      try {
//          inode = (INode)handler.handle();
//      } catch (IOException ex) {
//          throw new StorageException(ex.getMessage());
//      }
//    return inode;
//  }
//  public static int findINodeIdByBlockId(final long blockId) throws StorageException {
//    LOG.debug(String.format(
//            "About to read block with no transaction by bid=%d",
//            blockId));
//      LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.TEST) {
//          @Override
//          public Object performTask() throws PersistanceException, IOException {
//              BlockInfoDataAccess<BlockInfo> bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
//              BlockInfo bInfo = bda.findById(blockId);
//              return bInfo;
//          }
//      };
//    BlockInfo bInfo;
//      try {
//          bInfo = (BlockInfo)handler.handle();
//      } catch (IOException ex) {
//          throw new StorageException(ex.getMessage());
//      }
//    if (bInfo == null) {
//      return INode.NON_EXISTING_ID;
//    }
//    return bInfo.getInodeId();
//  }

  public static void findPathINodesById(int inodeId,LinkedList<INode> preTxResolvedINodes,boolean[] isPreTxPathFullyResolved) throws PersistanceException {
    if (inodeId != INode.NON_EXISTING_ID) {
      INode inode = indexINodeScanById(inodeId);
      if (inode == null) {
        isPreTxPathFullyResolved[0] = false;
        return;
      }
      preTxResolvedINodes.add(inode);
      readFromLeafToRoot(inode, preTxResolvedINodes);
    }
    isPreTxPathFullyResolved[0] = true;
    //reverse the list
    int firstCounter = 0;
    int lastCounter = preTxResolvedINodes.size()-1-firstCounter;
    INode firstNode = null;
    INode lastNode = null;
    while(firstCounter < (preTxResolvedINodes.size()/2)){
      firstNode = preTxResolvedINodes.get(firstCounter);
      lastNode = preTxResolvedINodes.get(lastCounter);
      preTxResolvedINodes.remove(firstCounter);
      preTxResolvedINodes.add(firstCounter, lastNode);
      preTxResolvedINodes.remove(lastCounter);
      preTxResolvedINodes.add(lastCounter, firstNode);
      firstCounter++;
      lastCounter = preTxResolvedINodes.size()-1-firstCounter;
    }
  }

  public static SortedSet<String> findPathsByLeaseHolder(String holder) throws StorageException {
    SortedSet<String> sortedPaths = new TreeSet<String>();
    LeaseDataAccess<Lease> lda = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
    Lease rcLease = lda.findByPKey(holder);
    if (rcLease == null) {
      return sortedPaths;
    }
    LeasePathDataAccess pda = (LeasePathDataAccess) StorageFactory.getDataAccess(LeasePathDataAccess.class);
    Collection<HopLeasePath> rclPaths = pda.findByHolderId(rcLease.getHolderID());
    for (HopLeasePath lp : rclPaths) {
      sortedPaths.add(lp.getPath()); // sorts paths in order to lock paths in the lexicographic order.
    }
    return sortedPaths;
  }

  private static INode getRoot() throws StorageException, PersistanceException {
    return  getNode(INodeDirectory.ROOT_NAME.getBytes(),INodeDirectory.ROOT_PARENT_ID, false);
  }

  public static INode indexINodeScanById(int id) throws StorageException {
    LOG.info(String.format(
            "Read inode with no transaction by id=%d",
            id));
    INodeDataAccess<INode> da = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
    return da.indexScanfindInodeById(id);
  }
 
  //puts the indoes in the list in reverse order
  private static void readFromLeafToRoot(INode inode, LinkedList<INode> list) throws PersistanceException {
    INode temp = inode;
    while (temp != null && temp.getParentId() != INodeDirectory.ROOT_PARENT_ID) {
      temp = indexINodeScanById(temp.getParentId()); // all upper components are dirs
      if(temp != null){
        list.add(temp);
      }
    }
  }
  
  public static INodeIdentifier resolveINodeFromBlockID(final long bid) throws StorageException{
    INodeIdentifier inodeIdentifier = null;
    LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws PersistanceException, IOException {
          
          BlockLookUpDataAccess<HopBlockLookUp> da = (BlockLookUpDataAccess) StorageFactory.getDataAccess(BlockLookUpDataAccess.class);
          HopBlockLookUp blu = da.findByBlockId(bid);
          if (blu == null) {
            return null;
          }
          return new INodeIdentifier(blu.getInodeId());
        }
      };      
      try {
        inodeIdentifier = (INodeIdentifier) handler.handle();
      } catch (IOException ex) {
        throw new StorageException(ex.getMessage());
      }
      return inodeIdentifier;
  }
  
  
  public static INodeIdentifier resolveINodeFromBlock(final Block b) throws StorageException{
    if (b instanceof BlockInfo || b instanceof BlockInfoUnderConstruction) {
      return new INodeIdentifier(((BlockInfo) b).getInodeId());
    } else {
      return resolveINodeFromBlockID(b.getBlockId());
    }
  }
}
