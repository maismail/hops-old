package se.sics.hop.common;

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
import org.apache.hadoop.hdfs.server.namenode.Lease;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.adaptor.INodeDALAdaptor;
import se.sics.hop.metadata.hdfs.dal.BlockLookUpDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopBlockLookUp;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author hooman
 */
public class INodeUtil {
  private final static Log LOG = LogFactory.getLog(INodeUtil.class);

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

  public static INode getNode(
      byte[] name,
      int parentId,
      boolean transactional)
      throws StorageException, TransactionContextException {
    String nameString = DFSUtil.bytes2String(name);
    if (transactional) {
      return EntityManager
          .find(INode.Finder.ByPK_NameAndParentId, nameString, parentId);
    } else {
      return findINodeWithNoTransaction(nameString, parentId);
    }
  }

  private static INode findINodeWithNoTransaction(
      String name,
      int parentId)
      throws StorageException {
    LOG.debug(String.format(
        "Read inode with no transaction by parent-id=%d, name=%s",
        parentId,
        name));
    INodeDataAccess<INode> da =
        (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
    return da.pkLookUpFindInodeByNameAndParentId(name, parentId);
  }

  public static void resolvePathWithNoTransaction(String path,
      boolean resolveLink, LinkedList<INode> preTxResolvedINodes,
      boolean[] isPathFullyResolved)
      throws UnresolvedPathException, StorageException,
      TransactionContextException {
    preTxResolvedINodes.clear();
    isPathFullyResolved[0] = false;

    byte[][] components = INode.getPathComponents(path);
    INode curNode = getRoot();
    preTxResolvedINodes.add(curNode);

    if (components.length == 1) {
      return;
    }

    INodeResolver resolver = new INodeResolver(components, curNode, resolveLink, false);
    while (resolver.hasNext()) {
      curNode = resolver.next();
      if (curNode != null) {
        preTxResolvedINodes.add(curNode);
      }
    }
    isPathFullyResolved[0] = preTxResolvedINodes.size() == components.length;
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

  public static void findPathINodesById(int inodeId,
      LinkedList<INode> preTxResolvedINodes, boolean[] isPreTxPathFullyResolved)
      throws StorageException {
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
    int lastCounter = preTxResolvedINodes.size() - 1 - firstCounter;
    INode firstNode = null;
    INode lastNode = null;
    while (firstCounter < (preTxResolvedINodes.size() / 2)) {
      firstNode = preTxResolvedINodes.get(firstCounter);
      lastNode = preTxResolvedINodes.get(lastCounter);
      preTxResolvedINodes.remove(firstCounter);
      preTxResolvedINodes.add(firstCounter, lastNode);
      preTxResolvedINodes.remove(lastCounter);
      preTxResolvedINodes.add(lastCounter, firstNode);
      firstCounter++;
      lastCounter = preTxResolvedINodes.size() - 1 - firstCounter;
    }
  }

  public static Set<String> findPathsByLeaseHolder(String holder)
      throws StorageException {
    HashSet<String> paths = new HashSet<String>();
    LeaseDataAccess<Lease> lda =
        (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
    Lease rcLease = lda.findByPKey(holder);
    if (rcLease == null) {
      return paths;
    }
    LeasePathDataAccess pda = (LeasePathDataAccess) StorageFactory
        .getDataAccess(LeasePathDataAccess.class);
    Collection<HopLeasePath> rclPaths =
        pda.findByHolderId(rcLease.getHolderID());
    for (HopLeasePath lp : rclPaths) {
      paths.add(lp.getPath());
    }
    return paths;
  }

  private static INode getRoot()
      throws StorageException, TransactionContextException {
    return getNode(INodeDirectory.ROOT_NAME.getBytes(),
        INodeDirectory.ROOT_PARENT_ID, false);
  }

  public static INode indexINodeScanById(int id) throws StorageException {
    LOG.debug(String.format(
        "Read inode with no transaction by id=%d",
        id));
    INodeDataAccess<INode> da =
        (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
    return da.indexScanfindInodeById(id);
  }

  //puts the indoes in the list in reverse order
  private static void readFromLeafToRoot(INode inode, LinkedList<INode> list)
      throws StorageException {
    INode temp = inode;
    while (temp != null &&
        temp.getParentId() != INodeDirectory.ROOT_PARENT_ID) {
      temp = indexINodeScanById(
          temp.getParentId()); // all upper components are dirs
      if (temp != null) {
        list.add(temp);
      }
    }
  }

  public static INodeIdentifier resolveINodeFromBlockID(final long bid)
      throws StorageException {
    INodeIdentifier inodeIdentifier;
    LightWeightRequestHandler handler = new LightWeightRequestHandler(
        HDFSOperationType.RESOLVE_INODE_FROM_BLOCKID) {
      @Override
      public Object performTask() throws IOException {

        BlockLookUpDataAccess<HopBlockLookUp> da =
            (BlockLookUpDataAccess) StorageFactory
                .getDataAccess(BlockLookUpDataAccess.class);
        HopBlockLookUp blu = da.findByBlockId(bid);
        if (blu == null) {
          return null;
        }
        INodeIdentifier inodeIdent = new INodeIdentifier(blu.getInodeId());
        INodeDALAdaptor ida = (INodeDALAdaptor) StorageFactory
            .getDataAccess(INodeDataAccess.class);
        INode inode = ida.indexScanfindInodeById(blu.getInodeId());
        if (inode != null) {
          inodeIdent.setName(inode.getLocalName());
          inodeIdent.setPid(inode.getParentId());
        }
        return inodeIdent;
      }
    };
    try {
      inodeIdentifier = (INodeIdentifier) handler.handle();
    } catch (IOException ex) {
      throw new StorageException(ex.getMessage());
    }
    return inodeIdentifier;
  }
  
  
  public static INodeIdentifier resolveINodeFromBlock(final Block b)
      throws StorageException {
    if (b instanceof BlockInfo || b instanceof BlockInfoUnderConstruction) {
      INodeIdentifier inodeIden =
          new INodeIdentifier(((BlockInfo) b).getInodeId());
      INodeDALAdaptor ida =
          (INodeDALAdaptor) StorageFactory.getDataAccess(INodeDataAccess.class);
      INode inode = ida.indexScanfindInodeById(((BlockInfo) b).getInodeId());
      if (inode != null) {
        inodeIden.setName(inode.getLocalName());
        inodeIden.setPid(inode.getParentId());
      }
      return inodeIden;
    } else {
      return resolveINodeFromBlockID(b.getBlockId());
    }
  }
  
  public static int[] resolveINodesFromBlockIds(final long[] blockIds)
      throws StorageException {
    LightWeightRequestHandler handler =
        new LightWeightRequestHandler(HDFSOperationType.GET_INODEIDS_FOR_BLKS) {
          @Override
          public Object performTask() throws IOException {
            BlockLookUpDataAccess<HopBlockLookUp> da =
                (BlockLookUpDataAccess) StorageFactory
                    .getDataAccess(BlockLookUpDataAccess.class);
            return da.findINodeIdsByBlockIds(blockIds);
          }
        };
    try {
      return (int[]) handler.handle();
    } catch (IOException ex) {
      throw new StorageException(ex.getMessage());
    }
  }

  public static INodeIdentifier resolveINodeFromId(final int id)
      throws StorageException {
    INodeIdentifier inodeIdentifier;

    LightWeightRequestHandler handler =
        new LightWeightRequestHandler(HDFSOperationType.RESOLVE_INODE_FROM_ID) {

          @Override
          public Object performTask() throws StorageException, IOException {
            INodeDALAdaptor ida = (INodeDALAdaptor) StorageFactory
                .getDataAccess(INodeDataAccess.class);
            INode inode = ida.indexScanfindInodeById(id);
            INodeIdentifier inodeIdent = new INodeIdentifier(id);
            if (inode != null) {
              inodeIdent.setName(inode.getLocalName());
              inodeIdent.setPid(inode.getParentId());
            }
            return inodeIdent;
          }
        };

    try {
      inodeIdentifier = (INodeIdentifier) handler.handle();
    } catch (IOException ex) {
      throw new StorageException(ex.getMessage());
    }
    return inodeIdentifier;
  }

  public static String constructPath(List<INode> pathINodes) {
    StringBuilder builder = new StringBuilder();
    for (INode node : pathINodes) {
      if (node.isDirectory()) {
        builder.append(node.getLocalName() + "/");
      } else {
        builder.append(node.getLocalName());
      }
    }
    return builder.toString();
  }
}
