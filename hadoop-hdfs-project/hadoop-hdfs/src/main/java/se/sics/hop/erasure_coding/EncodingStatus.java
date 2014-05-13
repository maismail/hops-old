package se.sics.hop.erasure_coding;

import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;

public class EncodingStatus {

  public static enum Status {
    NOT_ENCODED,
    ENCODING_REQUESTED,
    ENCODING_ACTIVE,
    ENCODING_CANCELED,
    ENCODING_FAILED,
    ENCODED,
    REPAIR_REQUESTED,
    REPAIR_ACTIVE,
    REPAIR_CANCELED,
    REPAIR_FAILED,
  }

  public static enum Counter implements CounterType<EncodingStatus> {
    RequestedEncodings,
    ActiveEncodings,
    ActiveRepairs,
    Encoded;

    @Override
    public Class getType() {
      return EncodingStatus.class;
    }
  }

  public static enum Finder implements FinderType<EncodingStatus> {
    ByInodeId,
    LimitedByStatusRequestedEncodings,
    ByStatusActiveEncodings,
    ByStatusActiveRepairs,
    LimitedByStatusEncoded,
    LimitedByStatusRequestedRepair;

    @Override
    public Class getType() {
      return EncodingStatus.class;
    }
  }

  private long inodeId;
  private Status status;
  private EncodingPolicy encodingPolicy;
  private long modificationTime;

  public EncodingStatus() {

  }

  public EncodingStatus(Status status) {
    this.status = status;
  }

  public EncodingStatus(Status status, EncodingPolicy encodingPolicy) {
    this.status = status;
    this.encodingPolicy = encodingPolicy;
  }

  public EncodingStatus(long inodeId, Status status, EncodingPolicy encodingPolicy, long modificationTime) {
    this.inodeId = inodeId;
    this.status = status;
    this.encodingPolicy = encodingPolicy;
    this.modificationTime = modificationTime;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  public EncodingPolicy getEncodingPolicy() {
    return encodingPolicy;
  }

  public void setEncodingPolicy(EncodingPolicy encodingPolicy) {
    this.encodingPolicy = encodingPolicy;
  }

  public boolean isEncoded() {
    switch (status) {
      case ENCODED:
      case REPAIR_REQUESTED:
      case REPAIR_ACTIVE:
      case REPAIR_CANCELED:
      case REPAIR_FAILED:
        return true;
      default:
        return false;
    }
  }

  public boolean isCorrupt() {
    switch (status) {
      case REPAIR_REQUESTED:
      case REPAIR_ACTIVE:
      case REPAIR_CANCELED:
      case REPAIR_FAILED:
        return true;
      default:
        return false;
    }
  }

  @Override
  public String toString() {
    return "EncodingStatus{" +
        "inodeId=" + inodeId +
        ", status=" + status +
        ", encodingPolicy=" + encodingPolicy +
        ", modificationTime=" + modificationTime +
        '}';
  }
}
