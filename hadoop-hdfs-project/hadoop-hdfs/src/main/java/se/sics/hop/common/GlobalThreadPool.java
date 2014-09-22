package se.sics.hop.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class GlobalThreadPool {

  private static ExecutorService executorService = Executors.newCachedThreadPool();

  private GlobalThreadPool() {
  }

  public static ExecutorService getExecutorService() {
    return executorService;
  }
}
