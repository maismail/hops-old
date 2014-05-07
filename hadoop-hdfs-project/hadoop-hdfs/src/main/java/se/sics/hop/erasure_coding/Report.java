package se.sics.hop.erasure_coding;

public class Report {

  private String filePath;
  private Status status;

  public enum Status {
    ACTIVE,
    FINISHED,
    FAILED,
    CANCELED
  }

  public Report(String filePath, Status status) {
    this.filePath = filePath;
    this.status = status;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }
}
