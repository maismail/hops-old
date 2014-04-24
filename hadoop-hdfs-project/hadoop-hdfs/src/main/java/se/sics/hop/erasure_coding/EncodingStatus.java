package se.sics.hop.erasure_coding;

public class EncodingStatus {

  public static enum Status {
    NOT_ENCODED,
    ENCODED,
    ENCODING_REQUESTED
  }

  private Status status;
  private String codec;

  public EncodingStatus() {
  }

  public EncodingStatus(Status status) {
    this.status = status;
  }

  public EncodingStatus(Status status, String codec) {
    this.status = status;
    this.codec = codec;
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
