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
    DELETED,
  }

  public static enum ParityStatus {
    HEALTHY,
    REPAIR_REQUESTED,
    REPAIR_ACTIVE,
    REPAIR_CANCELED,
    REPAIR_FAILED
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
    ByParityInodeId,
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

  private Integer inodeId;
  private Integer parityInodeId;
  private Status status;
  private ParityStatus parityStatus;
  private EncodingPolicy encodingPolicy;
  private Long statusModificationTime;
  private Long parityStatusModificationTime;
  private String parityFileName;
  private Integer lostBlocks;
  private Integer lostParityBlocks;
  private Boolean revoked;

  public EncodingStatus() {

  }

  public EncodingStatus(Status status) {
    this.status = status;
  }

  public EncodingStatus(Status status, EncodingPolicy encodingPolicy, String parityFileName) {
    this.status = status;
    this.encodingPolicy = encodingPolicy;
    this.parityFileName = parityFileName;
  }

  public EncodingStatus(Integer inodeId, Status status, EncodingPolicy encodingPolicy, Long statusModificationTime) {
    this.inodeId = inodeId;
    this.status = status;
    this.encodingPolicy = encodingPolicy;
    this.statusModificationTime = statusModificationTime;
  }

  public EncodingStatus(Integer inodeId, Integer parityInodeId, Status status, ParityStatus parityStatus,
      EncodingPolicy encodingPolicy, Long statusModificationTime, Long parityStatusModificationTime,
      String parityFileName, Integer lostBlocks, Integer lostParityBlocks, Boolean revoked) {
    this.inodeId = inodeId;
    this.parityInodeId = parityInodeId;
    this.status = status;
    this.parityStatus = parityStatus;
    this.encodingPolicy = encodingPolicy;
    this.statusModificationTime = statusModificationTime;
    this.parityStatusModificationTime = parityStatusModificationTime;
    this.parityFileName = parityFileName;
    this.lostBlocks = lostBlocks;
    this.lostParityBlocks = lostParityBlocks;
    this.revoked = revoked;
  }

  public Integer getInodeId() {
    return inodeId;
  }

  public void setInodeId(Integer inodeId) {
    this.inodeId = inodeId;
  }

  public Integer getParityInodeId() {
    return parityInodeId;
  }

  public void setParityInodeId(Integer parityInodeId) {
    this.parityInodeId = parityInodeId;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public ParityStatus getParityStatus() {
    return parityStatus;
  }

  public void setParityStatus(ParityStatus parityStatus) {
    this.parityStatus = parityStatus;
  }

  public EncodingPolicy getEncodingPolicy() {
    return encodingPolicy;
  }

  public void setEncodingPolicy(EncodingPolicy encodingPolicy) {
    this.encodingPolicy = encodingPolicy;
  }

  public Long getStatusModificationTime() {
    return statusModificationTime;
  }

  public void setStatusModificationTime(Long statusModificationTime) {
    this.statusModificationTime = statusModificationTime;
  }

  public Long getParityStatusModificationTime() {
    return parityStatusModificationTime;
  }

  public void setParityStatusModificationTime(Long parityStatusModificationTime) {
    this.parityStatusModificationTime = parityStatusModificationTime;
  }

  public String getParityFileName() {
    return parityFileName;
  }

  public void setParityFileName(String parityFileName) {
    this.parityFileName = parityFileName;
  }

  public Integer getLostBlocks() {
    return lostBlocks;
  }

  public void setLostBlocks(Integer lostBlocks) {
    this.lostBlocks = lostBlocks;
  }

  public Integer getLostParityBlocks() {
    return lostParityBlocks;
  }

  public void setLostParityBlocks(Integer lostParityBlocks) {
    this.lostParityBlocks = lostParityBlocks;
  }

  public Boolean getRevoked() {
    return revoked;
  }

  public void setRevoked(Boolean revoked) {
    this.revoked = revoked;
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

  public boolean isParityRepairActive() {
    switch (parityStatus) {
      case REPAIR_ACTIVE:
        return true;
      default:
        return false;
    }
  }

  public boolean isParityCorrupt() {
    switch (parityStatus) {
      case REPAIR_ACTIVE:
      case REPAIR_REQUESTED:
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
        ", parityInodeId=" + parityInodeId +
        ", status=" + status +
        ", parityStatus=" + parityStatus +
        ", encodingPolicy=" + encodingPolicy +
        ", statusModificationTime=" + statusModificationTime +
        ", parityStatusModificationTime=" + parityStatusModificationTime +
        ", parityFileName='" + parityFileName + '\'' +
        ", lostBlocks=" + lostBlocks +
        ", lostParityBlocks=" + lostParityBlocks +
        ", revoked=" + revoked +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EncodingStatus status1 = (EncodingStatus) o;

    if (encodingPolicy != null ? !encodingPolicy.equals(status1.encodingPolicy) : status1.encodingPolicy != null)
      return false;
    if (inodeId != null ? !inodeId.equals(status1.inodeId) : status1.inodeId != null) return false;
    if (lostBlocks != null ? !lostBlocks.equals(status1.lostBlocks) : status1.lostBlocks != null) return false;
    if (lostParityBlocks != null ? !lostParityBlocks.equals(
        status1.lostParityBlocks) : status1.lostParityBlocks != null)
      return false;
    if (parityFileName != null ? !parityFileName.equals(status1.parityFileName) : status1.parityFileName != null)
      return false;
    if (parityInodeId != null ? !parityInodeId.equals(status1.parityInodeId) : status1.parityInodeId != null)
      return false;
    if (parityStatus != status1.parityStatus) return false;
    if (parityStatusModificationTime != null ? !parityStatusModificationTime.equals(
        status1.parityStatusModificationTime) : status1.parityStatusModificationTime != null)
      return false;
    if (revoked != null ? !revoked.equals(status1.revoked) : status1.revoked != null) return false;
    if (status != status1.status) return false;
    if (statusModificationTime != null ? !statusModificationTime.equals(
        status1.statusModificationTime) : status1.statusModificationTime != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = inodeId != null ? inodeId.hashCode() : 0;
    result = 31 * result + (parityInodeId != null ? parityInodeId.hashCode() : 0);
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (parityStatus != null ? parityStatus.hashCode() : 0);
    result = 31 * result + (encodingPolicy != null ? encodingPolicy.hashCode() : 0);
    result = 31 * result + (statusModificationTime != null ? statusModificationTime.hashCode() : 0);
    result = 31 * result + (parityStatusModificationTime != null ? parityStatusModificationTime.hashCode() : 0);
    result = 31 * result + (parityFileName != null ? parityFileName.hashCode() : 0);
    result = 31 * result + (lostBlocks != null ? lostBlocks.hashCode() : 0);
    result = 31 * result + (lostParityBlocks != null ? lostParityBlocks.hashCode() : 0);
    result = 31 * result + (revoked != null ? revoked.hashCode() : 0);
    return result;
  }
}
