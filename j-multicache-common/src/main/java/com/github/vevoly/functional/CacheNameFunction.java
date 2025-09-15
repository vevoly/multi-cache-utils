package com.github.vevoly.functional;

@FunctionalInterface
public interface CacheNameFunction {
    String resolve(String key);
}
