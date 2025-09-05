package com.vevoly.multicache.plug;

/**
 * 缓存预热接口
 */
public interface CachePreloadable {

    /**
     * 执行缓存预热操作。
     * @return 成功预热的缓存条目数量。
     */
    int preLoadCache();

    /**
     * 提供一个名字，用于在日志中清晰地标识。
     */
    default String getPreloadName() {
        String className = this.getClass().getSimpleName();

        // 默认返回类名，去掉 "Service" 和 "Impl" 后缀
        return className.replaceAll("(?:ServiceImpl|Service|Impl)$", "");
    }

}
