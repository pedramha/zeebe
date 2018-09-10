package io.zeebe.logstreams.state;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public class RocksDbInternal {

  static Field columnFamilyHandle;
  static Field rocksDbNativeHandle;

  static Method putMethod;
  static Method putWithHandle;
  static Method getMethod;

  static {
    RocksDB.loadLibrary();

    try {
      resolveInternalMethods();
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void resolveInternalMethods() throws NoSuchFieldException, NoSuchMethodException {
    nativeHandles();

    getMethod();

    putMethod();
    putWithHandle();
  }

  private static void nativeHandles() throws NoSuchFieldException {
    columnFamilyHandle = ColumnFamilyHandle.class.getSuperclass().getDeclaredField("nativeHandle_");
    columnFamilyHandle.setAccessible(true);

    rocksDbNativeHandle = RocksDB.class.getSuperclass().getDeclaredField("nativeHandle_");
    rocksDbNativeHandle.setAccessible(true);
  }

  private static void getMethod() throws NoSuchMethodException {
    getMethod =
        RocksDB.class.getDeclaredMethod(
            "get",
            Long.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE);
    getMethod.setAccessible(true);
  }

  private static void putMethod() throws NoSuchMethodException {
    putMethod =
        RocksDB.class.getDeclaredMethod(
            "put",
            Long.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE);
    putMethod.setAccessible(true);
  }

  private static void putWithHandle() throws NoSuchMethodException {
    putWithHandle =
        RocksDB.class.getDeclaredMethod(
            "put",
            Long.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE,
            Long.TYPE);
    putWithHandle.setAccessible(true);
  }
}
