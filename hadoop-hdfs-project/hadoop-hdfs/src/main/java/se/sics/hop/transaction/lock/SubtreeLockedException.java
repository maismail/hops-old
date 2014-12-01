package se.sics.hop.transaction.lock;

import java.io.IOException;

public class SubtreeLockedException extends IOException {

  public SubtreeLockedException() {
  }

  public SubtreeLockedException(String message) {
    super(message);
  }

  public SubtreeLockedException(String message, Throwable cause) {
    super(message, cause);
  }

  public SubtreeLockedException(Throwable cause) {
    super(cause);
  }
}
