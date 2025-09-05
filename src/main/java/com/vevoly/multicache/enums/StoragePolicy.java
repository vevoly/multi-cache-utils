package com.vevoly.multicache.enums;

import lombok.AllArgsConstructor;

/**
 * 缓存策略枚举类
 * @Author: Vevoly
 */
@AllArgsConstructor
public enum StoragePolicy {

    /**
     * 默认：查L1, 查L2, L2命中后回填L1
     */
    L1_L2_DB(true, true, true),

    /**
     * 只查L1和DB, 不涉及L2
     */
    L1_DB(true, false, false),

    /**
     * 只查L2和DB, L2命中后【不】回填L1
     */
    L2_DB(false, true, false);

    private final boolean useL1;
    private final boolean useL2;
    private final boolean populateL1FromL2;

    public boolean isUseL1() { return useL1; }
    public boolean isUseL2() { return useL2; }
    public boolean isPopulateL1FromL2() { return populateL1FromL2; }
}
