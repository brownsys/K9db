package edu.brown.pelton;

import java.util.ArrayList;

public class PeltonJNI {
  static {
      System.loadLibrary("PeltonJNISO");
  }
  
  // Points to the state object from c++.
  private long connection = 0;

  // Opens a DB connection.
  private native void Open(String directory, String db_name, String username,
                           String password);
  public native boolean ExecuteDDL(String sql);
  public native int ExecuteUpdate(String sql);
  public native ArrayList<String[]> ExecuteQuery(String sql);
  
  // Closes the DB connection.
  public native void Close();
  
  public PeltonJNI(String directory, String db_name, String username,
                   String password) {
    this.Open(directory, db_name, username, password);
  }  
}
