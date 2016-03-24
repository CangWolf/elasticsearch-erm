/*
 * Copyright (C), 上海布鲁爱电子商务有限公司
 */
package com.github.cangwolf.erm.constants;

/**
 * @author Roy.Chen
 * @version $Id: ErmSearchType.java, v 0.1 2015-08-06 18:06
 */
public enum ErmSearchType {
    /**
     * and column = value
     */
    AND(1),
    /**
     * or column = value
     */
    OR(2),
    /**
     * and column like %value%
     */
    ANDLIKE(3),
    /**
     * or columnt like %value%
     */
    ORLIKE(4);

    private int type;

    ErmSearchType(int type) {
        this.type = type;
    }

    public static boolean isOr(ErmSearchType type) {
        return type.type%2 == 0;
    }


}
