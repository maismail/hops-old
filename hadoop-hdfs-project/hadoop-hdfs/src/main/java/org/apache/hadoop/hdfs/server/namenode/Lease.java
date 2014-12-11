package org.apache.hadoop.hdfs.server.namenode;

import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;

import java.util.Collection;
import java.util.TreeSet;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.StorageException;

/**
 * **********************************************************
 * A Lease governs all the locks held by a single client. For each client
 * there's a corresponding lease, whose timestamp is updated when the client
 * periodically checks in. If the client dies and allows its lease to expire,
 * all the corresponding locks can be released.
 * ***********************************************************
 */
public class Lease implements Comparable<Lease> {

  public static enum Counter implements CounterType<Lease> {

    All;

    @Override
    public Class getType() {
      return Lease.class;
    }
  }

  public static enum Finder implements FinderType<Lease> {

    ByPKey, ByHolderId, All, ByTimeLimit;

    @Override
    public Class getType() {
      return Lease.class;
    }
  }
  private final String holder;
  private long lastUpdate;
  private Collection<HopLeasePath> paths = null;
  private int holderID; 

  public Lease(String holder, int holderID, long lastUpd) {
    this.holder = holder;
    this.holderID = holderID;
    this.lastUpdate = lastUpd;
  }

  public void setLastUpdate(long lastUpd) {
    this.lastUpdate = lastUpd;
  }

  public void setPaths(TreeSet<HopLeasePath> paths) {
    this.paths = paths;
  }
  
  public long getLastUpdate() {
    return this.lastUpdate;
  }

  public void setHolderID(int holderID) {
    this.holderID = holderID;
  }

  public int getHolderID() {
    return this.holderID;
  }

  public boolean removePath(HopLeasePath lPath)
      throws StorageException, TransactionContextException {
    return getPaths().remove(lPath);
  }

  public void addPath(HopLeasePath lPath)
      throws StorageException, TransactionContextException {
    getPaths().add(lPath);
  }

  public void addFirstPath(HopLeasePath lPath) {
    this.paths = new TreeSet<HopLeasePath>();
    this.paths.add(lPath);
  }

  /**
   * Does this lease contain any path?
   */
  boolean hasPath() throws StorageException, TransactionContextException {
    return !this.getPaths().isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    int size = 0;
    if (paths != null) {
      size = paths.size();
    }
    return "[Lease.  Holder: " + holder
            + ", pendingcreates: " + size + "]";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(Lease o) {
    Lease l1 = this;
    Lease l2 = o;
    long lu1 = l1.lastUpdate;
    long lu2 = l2.lastUpdate;
    if (lu1 < lu2) {
      return -1;
    } else if (lu1 > lu2) {
      return 1;
    } else {
      return l1.holder.compareTo(l2.holder);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Lease)) {
      return false;
    }
    Lease obj = (Lease) o;
    if (lastUpdate == obj.lastUpdate
            && holder.equals(obj.holder)) {
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return holder.hashCode();
  }

  public Collection<HopLeasePath> getPaths()
      throws StorageException, TransactionContextException {
    if (paths == null) {
      paths = EntityManager.findList(HopLeasePath.Finder.ByHolderId, holderID);
    }

    return paths;
  }

  public String getHolder() {
    return holder;
  }

  void replacePath(HopLeasePath oldpath, HopLeasePath newpath) throws
      StorageException, TransactionContextException {
    getPaths().remove(oldpath);
    getPaths().add(newpath);
  }
}