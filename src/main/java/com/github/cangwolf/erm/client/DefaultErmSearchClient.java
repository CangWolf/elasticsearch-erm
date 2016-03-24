package com.github.cangwolf.erm.client;

import com.github.cangwolf.erm.model.BaseModel;
import com.github.cangwolf.erm.util.BuilderUtil;
import com.google.gson.Gson;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by roy on 16-3-9.
 */
public class DefaultErmSearchClient<T extends BaseModel> extends AbstractSearchClient<T>{

    private Class<? extends BaseModel> clz;

    public DefaultErmSearchClient(String index, String type) {
        super(index, type);
    }

    public DefaultErmSearchClient(String host, int tcpport, String index, String type) {
        super(host, tcpport, index, type);
    }

    public DefaultErmSearchClient(String host, int tcpport, String index, String type, Class<? extends BaseModel> clz) {
        super(host, tcpport, index, type);
        this.clz = clz;
    }

    @Override
    public T build(SearchHit hit) {
        Gson gson = new Gson();
        return  (T)gson.fromJson(gson.toJson(hit.getSource()), clz);
    }

    @Override
    public XContentBuilder getXContentBuilder(BaseModel vo) {
        try {
            return BuilderUtil.buildObject(jsonBuilder(), null, vo, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getPrimaryKeyName() {
        return "id";
    }
}
