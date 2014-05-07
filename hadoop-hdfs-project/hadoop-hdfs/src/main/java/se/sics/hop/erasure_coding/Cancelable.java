package se.sics.hop.erasure_coding;

public interface Cancelable<T> {

  public void cancelAll();
  public void cancel(T toCancel);
}
