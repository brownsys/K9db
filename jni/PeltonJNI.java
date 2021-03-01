package edu.brown.pelton;

public class PeltonJNI {
  static {
      System.loadLibrary("PeltonJNISO");
  }

  // Declare a native method sayHello() that receives no arguments and returns void
  public native void sayHello();
}
