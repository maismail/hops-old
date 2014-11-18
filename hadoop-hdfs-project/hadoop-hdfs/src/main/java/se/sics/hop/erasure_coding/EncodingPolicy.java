package se.sics.hop.erasure_coding;

public class EncodingPolicy {
  private String codec;
  private short targetReplication;

  public EncodingPolicy(String codec, short targetReplication) {
    setCodec(codec);
    setTargetReplication(targetReplication);
  }

  public void setCodec(String codec) {
    if (codec.length() > 8) {
      throw new IllegalArgumentException("Codec cannot have more than 8 characters");
    }
    this.codec = codec;
  }

  public void setTargetReplication(short targetReplication) {
    this.targetReplication = targetReplication;
  }

  public String getCodec() {
    return codec;
  }

  public short getTargetReplication() {
    return targetReplication;
  }

  @Override
  public String toString() {
    return "EncodingPolicy{" +
        "codec='" + codec + '\'' +
        ", targetReplication=" + targetReplication +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EncodingPolicy policy = (EncodingPolicy) o;

    if (targetReplication != policy.targetReplication) return false;
    if (!codec.equals(policy.codec)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = codec.hashCode();
    result = 31 * result + targetReplication;
    return result;
  }
}
