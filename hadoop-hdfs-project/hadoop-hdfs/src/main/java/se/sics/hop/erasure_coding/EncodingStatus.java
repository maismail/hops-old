package se.sics.hop.erasure_coding;

import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;

public class EncodingStatus {

  public static enum Status {
    NOT_ENCODED,
    ENCODING_REQUESTED,
    ENCODING_ACTIVE,
    ENCODED,
    REPAIR_ACTIVE
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
    LimitedByStatusRequestEncodings,
    ByStatusActiveEncodings,
    ByStatusActiveRepairs,
    LimitedByStatusEncoded;

    @Override
    public Class getType() {
      return EncodingStatus.class;
    }
  }

  private long inodeId;
  private Status status;
  private String codec;
  private long modificationTime;

  public EncodingStatus() {

  }

  public EncodingStatus(Status status) {
    this.status = status;
  }

  public EncodingStatus(Status status, String codec) {
    this.status = status;
    this.codec = codec;
  }

  public EncodingStatus(long inodeId, Status status, String codec, long modificationTime) {
    this.inodeId = inodeId;
    this.status = status;
    this.codec = codec;
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

  public void setCodec(String codec) {
    this.codec = codec;
  }

  public Status getStatus() {
    return status;
  }

  public String getCodec() {
    return codec;
  }

  public boolean isEncoded() {
    return status == Status.ENCODED;
  }
}
