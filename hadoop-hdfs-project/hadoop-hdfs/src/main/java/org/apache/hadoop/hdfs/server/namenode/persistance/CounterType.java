package org.apache.hadoop.hdfs.server.namenode.persistance;


/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public interface CounterType<T> {

  public Class getType();
}