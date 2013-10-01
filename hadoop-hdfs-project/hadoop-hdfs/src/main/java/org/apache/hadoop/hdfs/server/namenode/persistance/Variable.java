package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.EnumMap;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class Variable {

  public final static EnumMap<Finder, Long> defaultValues = new EnumMap(Finder.class);

  public static void registerVariableDefaultValue(Finder variable, long defaultValue) {
    defaultValues.put(variable, defaultValue);
  }

  public static enum Finder implements FinderType<Variable> {

    GenerationStamp;

    public int getId() {
      return this.ordinal();
    }

    public long getDefaultValue() {
      return defaultValues.get(this);
    }

    @Override
    public Class getType() {
      return Variable.class;
    }
  }
  private final Finder type;
  private long value;

  public Variable(Finder type, long value) {
    this.type = type;
    this.value = value;
  }

  public Variable(Finder type) {
    this(type, 0);
  }

  public void setValue(Long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  public Finder getType() {
    return type;
  }
}
