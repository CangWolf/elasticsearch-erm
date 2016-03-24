package com.github.cangwolf.erm.util;

import com.thebeastshop.pegasus.service.pub.vo.PsChnProdSaleSku;
import com.thebeastshop.pegasus.service.pub.vo.PsDeliveryVO;
import com.thebeastshop.pegasus.service.pub.vo.PsDynmContentVO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by roy on 16-2-25.
 */
public class BuilderUtil {

    /**
     * @param builder
     * @param fieldName
     * @param object
     * @param cascade   if object has a collection field , is start an array
     * @return
     */
    public static XContentBuilder buildObject(XContentBuilder builder, String fieldName, Object object, boolean cascade) {
        try {
            Map<String, Object> nameValueMap = ReflectionUtil.getFieldNameValue(object);
            if (StringUtils.isNotBlank(fieldName)) {
                builder.startObject(fieldName);
            } else {
                builder.startObject();
            }
            for (Map.Entry<String, Object> nventry : nameValueMap.entrySet()) {
                String name = nventry.getKey();
                Object value = nventry.getValue();
                if (value instanceof List) {
                    if (cascade && (!isBaseTypeList((List) value))) {
                        buildArray(builder, name, value);
                    } else {
                        builder.field(name, value);
                    }
                } else {
                    builder.field(name, value);
                }
            }
            builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return builder;
    }

    public static XContentBuilder buildField(XContentBuilder builder, String fieldName, Object value) {
        try {
            builder.field(fieldName, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return builder;
    }

    public static XContentBuilder buildObject(XContentBuilder builder, String fieldName, Object object) {
        return buildObject(builder, fieldName, object, false);
    }

    public static XContentBuilder buildArray(XContentBuilder builder, String fieldName, Object object) {
        if (!(object instanceof List)) {
            return builder;
        } else {
            try {
                if (StringUtils.isNotBlank(fieldName)) {
                    builder.startArray(fieldName);
                } else {
                    builder.startArray();
                }
                if (CollectionUtils.isNotEmpty((Collection) object)) {
                    boolean isbase = false;
                    Object value = ((List) object).get(0);
                    if (value instanceof PsChnProdSaleSku) {
                    } else if (value instanceof PsChnProdSaleSku) {
                    } else if (value instanceof PsDynmContentVO) {
                    } else if (value instanceof PsDeliveryVO) {
                    } else {
                        isbase = true;
                    }
                    if (isbase) {
                        buildField(builder, fieldName, object);
                    } else {
                        for (int i = 0; i < ((List) object).size(); i++) {
                            Object value1 = ((List) object).get(i);
                            buildObject(builder, null, value1);
                        }
                    }
                } else {
                    buildObject(builder, fieldName, object);
                }
                builder.endArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return builder;
    }

    public static boolean isBaseTypeList(List objs) {
        boolean isbase = false;
        if (CollectionUtils.isEmpty(objs)) {
            isbase = true;
        } else {
            Object value = objs.get(0);
            if (value instanceof PsChnProdSaleSku) {
            } else if (value instanceof PsChnProdSaleSku) {
            } else if (value instanceof PsDynmContentVO) {
            } else if (value instanceof PsDeliveryVO) {
            } else {
                isbase = true;
            }
        }
        return isbase;
    }
}
