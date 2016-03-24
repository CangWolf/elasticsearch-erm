package com.github.cangwolf.erm.client;

import com.github.cangwolf.erm.constants.ErmSearchType;
import com.github.cangwolf.erm.exception.ErmException;
import com.github.cangwolf.erm.exception.ErmExceptionErrorCodes;
import com.github.cangwolf.erm.model.BaseModel;
import com.github.cangwolf.erm.model.BaseUpdateModel;
import com.github.cangwolf.erm.util.DateUtil;
import com.github.cangwolf.erm.util.ReflectionUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by roy on 16-3-9.
 */
public abstract class AbstractSearchClient<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractSearchClient.class);
    /**
     * ES host ip or name
     */
    private String host;
    /**
     * ES index
     */
    private String index;
    /**
     * ES type
     */
    private String type;
    /**
     * ES tcp port
     */
    private int tcpport;

    private TransportClient client;


    //default
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9300;

    //default page
    private static final int DEFAULT_FROM = 0;
    private static final int DEFAULT_PAGE_SIZE = 20;
    /**
     * es 2.x 开始 一次最多只能设置 10000
     */
    private static final int DEFAULT_PAGE_MAX_SIZE = 10000;

    public AbstractSearchClient(String index, String type) {
        this(index, DEFAULT_PORT, type, DEFAULT_HOST);
    }

    public AbstractSearchClient(String host, int tcpport, String index, String type) {
        this.host = host;
        this.index = index;
        this.type = type;
        this.tcpport = tcpport;
        init();
    }

    private void init() {
        try {
            client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), tcpport));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private void createMapping() {
        PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type);
        PutMappingResponse response = client.admin().indices().putMapping(mappingRequest).actionGet();
    }

    /**
     * @param name
     * @param value
     * @param count 需要返回的最多rows
     * @return
     */
    public List<T> findByField(String name, Object value, int count) {
        if (count < 1) count = DEFAULT_PAGE_SIZE;
        QueryBuilder builder = null;
        if (StringUtils.isBlank(name)) {
            builder = QueryBuilders.matchAllQuery();
        } else {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            builder = boolQueryBuilder;
            boolQueryBuilder.must(QueryBuilders.multiMatchQuery(value, name).type(MatchQueryBuilder.Type.PHRASE_PREFIX));
//            boolQueryBuilder.should(QueryBuilders.matchQuery(name, value).minimumShouldMatch("1"));
//            boolQueryBuilder.should(QueryBuilders.nestedQuery(name, QueryBuilders.queryStringQuery(value.toString())));
//            QueryBuilders.matchQuery(name, QueryBuilders.queryStringQuery(value.toString()).defaultOperator(QueryStringQueryBuilder.Operator.AND));
//            builder = QueryBuilders.termsQuery(name, value).minimumShouldMatch("1");

        }
        List<T> result = searchByQryBuilder(builder, DEFAULT_FROM, count);
        return result;
    }

    public List<T> findByField(String name, Object value) {
        return findByField(name, value, 0);
    }

    public List<T> searchByQryBuilder(QueryBuilder queryBuilder, int from, int size) {
        List<T> list = new ArrayList<T>();
        try {
            SearchResponse searchResponse = client.prepareSearch(index).setTypes(type)
                    .setQuery(queryBuilder)
                    .setFrom(from)
                    .setSize(size)
//                    .setRequestCache(true)
//                    .setQueryCache(false)  //delete from 2.x
                    .addSort(SortBuilders.fieldSort("id").order(SortOrder.ASC))
                    .execute()
                    .actionGet();
            SearchHits hits = searchResponse.getHits();
            log.info("查询到记录数=" + hits.getTotalHits());
            SearchHit[] searchHists = hits.getHits();
            if (searchHists.length > 0) {
                for (SearchHit hit : searchHists) {
                    list.add(build(hit));
                }
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new ErmException(ErmExceptionErrorCodes.SEARCH_ERROR, "查询出错！");
        }
        return list;
    }

    //ROYS  don't use it  TODO
    private List<T> searchByQryBuilderJoin(QueryBuilder queryBuilder, int from, int size) {
        List<T> list = new ArrayList<T>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        QueryBuilders.disMaxQuery();
        QueryBuilders.prefixQuery("", "110");
        try {
            SearchResponse searchResponse = client.prepareSearch(index).setTypes(type, "pubsku")
                    .setSearchType(org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(queryBuilder)
//                    .setPostFilter(FilterBuilders.boolFilter().should(FilterBuilders.termFilter("channelOffShelfList", "teststststs")))
                    .setFrom(from)
                    .setSize(size)
//                    .setFetchSource(null, null)
//                    .setQueryCache(false)
                    .addSort(SortBuilders.fieldSort("id").order(SortOrder.ASC))
                    .execute()
                    .actionGet();
            SearchHits hits = searchResponse.getHits();
            log.info("查询到记录数=" + hits.getTotalHits());
            SearchHit[] searchHists = hits.getHits();
            if (searchHists.length > 0) {
                for (SearchHit hit : searchHists) {
                    list.add(build(hit));
                }
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new ErmException(ErmExceptionErrorCodes.SEARCH_ERROR, "查询出错！");
        }
        return list;
    }

    /**
     * @param params
     * @param type   条件类型
     * @return
     */
    public List<T> findByFields(Map<String, Object> params, ErmSearchType type) {
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery(params.values().toString());
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        Integer currage = (Integer) params.remove("currpage");
        Integer pagenum = (Integer) params.remove("pagenum");

        //ROYS pre-process sth update time
        queryCondPrepare(boolQueryBuilder, params, type);
        for (Map.Entry<String, Object> entry : params.entrySet()) {

            String key = entry.getKey();
            Object val = entry.getValue();
            if (StringUtils.startsWith(key, "List")) {
                boolQueryBuilder.must(QueryBuilders.queryStringQuery(val.toString()).field(key));
                continue;
            }

            if (val instanceof List) {
                BoolQueryBuilder orBuilder = QueryBuilders.boolQuery();
                for (Object obj : (List) val) {
                    orBuilder.should(QueryBuilders.queryStringQuery(obj.toString()).field(key));
                }
                if (ErmSearchType.isOr(type)) {
                    boolQueryBuilder.should(orBuilder);
                } else {
                    boolQueryBuilder.must(orBuilder);
                }
                continue;
            }
            if (ErmSearchType.OR.equals(type)) {
                boolQueryBuilder.should(QueryBuilders.termQuery(key, val));
            } else if (ErmSearchType.AND.equals(type)) {
                boolQueryBuilder.must(QueryBuilders.termQuery(key, val));
            } else if (ErmSearchType.ORLIKE.equals(type)) {
                boolQueryBuilder.should(QueryBuilders.multiMatchQuery(val, key).type(MatchQueryBuilder.Type.PHRASE_PREFIX));
            } else if (ErmSearchType.ANDLIKE.equals(type)) {
                boolQueryBuilder.must(QueryBuilders.multiMatchQuery(val, key).type(MatchQueryBuilder.Type.PHRASE_PREFIX));
            }
        }

        int size = pagenum != null && pagenum > 0 ? pagenum : DEFAULT_PAGE_SIZE;
        int start = currage != null && currage > 0 ? (currage - 1) * pagenum : DEFAULT_FROM;
        List<T> result = searchByQryBuilder(boolQueryBuilder, start, size);
        return result;

    }

    public boolean create(T vo) {
        String primaryKeyName = getPrimaryKeyName();
        updateTime(vo);
        Object primaryKeyValue = ReflectionUtil.getFieldValue(vo, primaryKeyName);
        IndexResponse response = client.prepareIndex(index, type).setRefresh(true)
                .setSource(getXContentBuilder(vo))
                .setId(primaryKeyValue.toString())
                .execute().actionGet();
        return response.isCreated();
    }

    /**
     * batch create
     *
     * @param vos
     * @return
     */
    public boolean create(List<T> vos) {
        if (CollectionUtils.isEmpty(vos)) {
            return false;
        }
//        //创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
//        IndexRequestBuilder requestBuilder = client.prepareIndex(index, type).setRefresh(true);
        BulkRequestBuilder bulkBuilder = client.prepareBulk().setRefresh(true);
        for (T vo : vos) {
            updateTime(vo);
            Object primaryKeyValue = getPrimaryKeyValue(vo);
            bulkBuilder.add(client.prepareIndex(index, type).setRefresh(true).setSource(getXContentBuilder(vo)).setId(primaryKeyValue.toString()));
        }
        BulkResponse responses = bulkBuilder.execute().actionGet();
        return !responses.hasFailures();
    }

    public boolean deleteByPrimaryKey(Object id) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setRefresh(true);
        bulkRequestBuilder.add(new DeleteRequest(index, type, id.toString()));
        return !bulkRequestBuilder.execute().actionGet().hasFailures();
    }

    public boolean deleteByPrimaryKey(List<String> ids) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setRefresh(true);
        for (String id : ids) {
            bulkRequestBuilder.add(new DeleteRequest(index, type, id.toString()));
        }
        return !bulkRequestBuilder.execute().actionGet().hasFailures();
    }

    boolean deleteIndex() {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        ListenableActionFuture<DeleteIndexResponse> result = client.admin().indices().prepareDelete(index).execute();
        try {
            return result.get().isAcknowledged();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean createOrUpdate(T vo) {
        String primaryKeyName = getPrimaryKeyName();
        Object primaryKeyValue = ReflectionUtil.getFieldValue(vo, primaryKeyName);
        List<T> exists = findByField(primaryKeyName, primaryKeyValue);
        if (CollectionUtils.isEmpty(exists)) {
            return create(vo);
        }
        T exist = exists.get(0);
        return update(exist);
    }

    public boolean createOrUpdate(List<T> vos) {
        if (CollectionUtils.isEmpty(vos)) {
            return Boolean.TRUE;
        }
        for (T vo : vos) {
            createOrUpdate(vo);
        }
        return Boolean.TRUE;
    }

    public boolean update(List<T> vos) {
        BulkRequestBuilder bulkBuilder = client.prepareBulk().setRefresh(true);
        for (T vo : vos) {
            bulkBuilder.add(buildUpdateRequest(vo));
        }
        return !bulkBuilder.execute().actionGet().hasFailures();
    }

    public boolean update(T vo) {
        String primaryKeyName = getPrimaryKeyName();
        Object primaryKeyValue = getPrimaryKeyValue(vo);
        if (primaryKeyValue == null || StringUtils.isBlank(primaryKeyValue.toString())) {
            throw new ErmException(ErmExceptionErrorCodes.ILLEGAL_PARAMETER_IS_NULL, "主键不能为空！");
        }
        Map<String, Object> params = ReflectionUtil.getFieldNameValue(vo);
        if (MapUtils.isEmpty(params)) {
            return true;
        }
        try {
            UpdateResponse response = client.update(buildUpdateRequest(vo)).actionGet();
            return response.getVersion() > 0;
        } catch (Exception e) {
            log.error("", e);
            throw new ErmException(ErmExceptionErrorCodes.UPDATE_ERROR, "更新出错！");
        }
    }


    private UpdateRequest buildUpdateRequest(T vo) {
        Object primaryKeyValue = getPrimaryKeyValue(vo);
        if (primaryKeyValue == null) {
            throw new ErmException(ErmExceptionErrorCodes.ILLEGAL_PARAMETER_IS_NULL, "主键不能为空");
        }

        updateTime(vo);
        Map<String, Object> params = ReflectionUtil.getFieldNameValue(vo);
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(primaryKeyValue.toString());
        updateRequest.refresh(true);

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, (String) primaryKeyValue).setRefresh(true);
        params.remove(getPrimaryKeyName());
        XContentBuilder builder = null;

        try {
            builder = XContentFactory.jsonBuilder();
            //ROYS optimize
            String updateTime = (String) params.get("updateTime");
            if (StringUtils.isNotBlank(updateTime)) {
                params.put("updateLong", DateUtils.parseDate(updateTime, DateUtil.DEFAULT_DATETIME_FORMAT).getTime());
            }
            buildUpdateParams(builder, params);
            updateRequest.doc(builder);
        } catch (IOException e) {
            e.printStackTrace();
//            throw new PublicException(PublicExceptionErrorCode.UPDATE_ERROR, "批量更新出错！");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return updateRequest;
    }

    private void buildUpdateParams(XContentBuilder builder, Map<String, Object> params) {
        try {
            builder.startObject();
            for (String key : params.keySet()) {
                Object val = params.get(key);
                if (val instanceof List) {
                    List vos = (List) val;
                    if (CollectionUtils.isNotEmpty(vos) && vos.get(0) instanceof BaseModel) {
                        builder.startArray(key);
                        for (Object obj : vos) {
                            updateTime(obj);
                            buildUpdateParams(builder, ReflectionUtil.getFieldNameValue(obj));
                        }
                        builder.endArray();
                        continue;
                    }
                }
                builder.field(key, val);
            }
            builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Object getPrimaryKeyValue(T vo) {
        return ReflectionUtil.getFieldValue(vo, getPrimaryKeyName());
    }

    private void updateTime(Object obj) {
        if (obj instanceof BaseUpdateModel) {
            ((BaseUpdateModel) obj).setUpdateTime(getNow());
        }
    }

    String getNow() {
        return DateUtil.format(DateUtil.getNow(), DateUtil.DEFAULT_DATETIME_FORMAT);
    }

    //组装VO
    public abstract T build(SearchHit hit);

    //设置插入数据的名称，类型
    public abstract XContentBuilder getXContentBuilder(T vo);

    //获取字段的主键
    public abstract String getPrimaryKeyName();

    //可以做一些自定义处理，比如日期，范围
    public void queryCondPrepare(BoolQueryBuilder queryBuilder, Map<String, Object> params, ErmSearchType type) {
        String updateTime = (String) params.get("updateTime");
//        String updateTimeEnd = (String) params.get("updateTimeEnd");
        String updateTimeEnd = getNow();
        if (StringUtils.isNotBlank(updateTime)) {
//            RangeQueryBuilder rangeBuilder = QueryBuilders.rangeQuery("updateTime");
            RangeQueryBuilder rangeBuilder = QueryBuilders.rangeQuery("updateLong");
//            rangeBuilder.from(updateTime).to(updateTimeEnd);
            rangeBuilder.gte(DateUtil.parse(updateTime, DateUtil.DEFAULT_DATETIME_FORMAT).getTime());
            if (ErmSearchType.isOr(type)) {
                queryBuilder.should(rangeBuilder);
            } else {
                queryBuilder.must(rangeBuilder);
            }
        }
        params.remove("updateTime");
    }

    void addQueryBuilder(BoolQueryBuilder boolQueryBuilder, QueryBuilder queryBuilder, ErmSearchType type) {
        if (ErmSearchType.isOr(type)) {
            boolQueryBuilder.should(queryBuilder);
        } else {
            boolQueryBuilder.must(queryBuilder);
        }
    }


}