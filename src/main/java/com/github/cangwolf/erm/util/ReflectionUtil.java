package com.github.cangwolf.erm.util;

import com.github.cangwolf.erm.constants.FieldType;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by roy on 16-3-9.
 */
public class ReflectionUtil {
    public static Map<String, Object> getFieldNameValue(Object obj) {
        if (obj == null) return Collections.emptyMap();
        Map<String, Object> params = new HashMap<String, Object>();
        for (Class clz = obj.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
            Field[] fields = clz.getDeclaredFields();
            for (Field field : fields) {
                try {
                    field.setAccessible(true);
                    Object value = field.get(obj);
                    if (value != null) {
                        if (field.getType() == String.class) {
                            if (StringUtils.isNotEmpty((String) value)) {
                                params.put(field.getName(), StringUtils.trim((String) value));
                            }
                        } else {
                            params.put(field.getName(), value);
                        }
                    }
                    field.setAccessible(false);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return params;
    }

    public static Map<String, Object> getFieldNameStringValue(Object obj) {
        if (obj == null) return Collections.emptyMap();
        Map<String, Object> params = new HashMap<String, Object>();
        for (Class clz = obj.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
            Field[] fields = clz.getDeclaredFields();
            for (Field field : fields) {
                try {
                    field.setAccessible(true);
                    Object value = field.get(obj);
                    if (value != null) {
//                        if( StringUtils.isNotEmpty(value.toString())){
                        params.put(field.getName(), StringUtils.trim(value.toString()));
//                        }
                    }
                    field.setAccessible(false);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return params;
    }

    public static List<String> getAllFieldName(Object obj) {
        if (obj == null) return Collections.emptyList();
        Map<String, Object> params = new HashMap<String, Object>();
        List<String> fieldNames = new ArrayList<String>();
        for (Class clz = obj.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
            Field[] fields = clz.getDeclaredFields();
            for (Field field : fields) {
                fieldNames.add(field.getName());
            }
        }
        return fieldNames;
    }

    public static Object getFieldValue(Object obj, String fieldName) {
        if (obj == null) return null;
        for (Class clz = obj.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
            Field[] fields = clz.getDeclaredFields();
            for (Field field : fields) {
                try {
                    if (field.getName().equals(fieldName)) {
                        field.setAccessible(true);
                        return field.get(obj);
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public static boolean setFieldValue(Object obj, String fieldName, Object fieldValue) {
        if (obj == null) return true;
        for (Class clz = obj.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
            Field[] fields = clz.getDeclaredFields();
            for (Field field : fields) {
                try {
                    if (field.getName().equals(fieldName)) {
                        field.setAccessible(true);
                        field.set(obj, fieldValue);
                        return true;
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        return false;
    }

    public static XContentBuilder buildTypeValue(XContentBuilder builder, Object obj) {
        Field[] fields = obj.getClass().getDeclaredFields();
        for (Field field : fields) {
            try {
                Object value = field.get(obj);
                if (value != null) {
                    builder.field(field.getName(), value).field("type", FieldType.type(value.getClass()));
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return builder;
    }
}
