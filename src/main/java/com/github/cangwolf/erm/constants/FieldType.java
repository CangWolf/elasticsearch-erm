package com.github.cangwolf.erm.constants;

import java.util.ArrayList;
import java.util.Date;

/**
 * @author Roy.Chen
 * @version $Id: FieldType.java, v 0.1 2015-08-05 17:21
 */
public enum FieldType {
    LONG(Long.class,"long"),
    INTEGER(Integer.class,"int"),
    DOUBLE(Double.class,"double"),
    STRING(String.class,"string"),
    ArrayList(ArrayList.class,"list"),
    DATE(Date.class,"date");

    private Class clz;
    private String type;

    private FieldType(Class clz, String type) {
        this.clz = clz;
        this.type = type;
    }

    /**
     * 查找对象类型
     * @param obj
     * @return
     */
    public static String type(Object obj) {
        if(obj == null) return null;
        Class clz = obj.getClass();
        for (FieldType type : FieldType.values()) {
            if (clz == type.clz) {
                return type.type;
            }
        }

        return null;
    }

}
