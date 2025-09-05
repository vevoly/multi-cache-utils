package com.vevoly.multicache.functional;

@FunctionalInterface
public interface CacheNameFunction {
    String resolve(String key);
}
