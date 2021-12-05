package edu.brown.pelton;

public class MemcachedJNI {
  static {
      System.loadLibrary("MemcachedJNI_SO");
  }

  // Represents a value.
  public static class MemcachedValue {
    public static int STRING = 1;
    public static int INT64 = 2;
    public static int UINT64 = 3;

    int type;
    private String str;
    private long int64;
    private long uint64;

    private MemcachedValue() {}

    public static MemcachedValue fromStr(String s) {
      MemcachedValue v = new MemcachedValue();
      v.type = MemcachedValue.STRING;
      v.str = s;
      return v;
    }
    public static MemcachedValue fromInt(long s) {
      MemcachedValue v = new MemcachedValue();
      v.type = MemcachedValue.INT64;
      v.int64 = s;
      return v;
    }
    public static MemcachedValue fromUInt(long s) {
      MemcachedValue v = new MemcachedValue();
      v.type = MemcachedValue.UINT64;
      v.uint64 = s;
      return v;
    }

    public int getType() {
      return this.type;
    }
    public String getStr() {
      return this.str;
    }
    public long getInt() {
      return this.int64;
    }
    public long getUInt() {
      return this.uint64;
    }
  }

  public static native void Initialize(String db, String username, String password, String seed);
  public static native int Cache(String query);
  public static native void Update(String table);
  public static native MemcachedValue[][] Read(int id, MemcachedValue[] key);

  // For tests.
  public static native void __ExecuteDB(String query);
}


