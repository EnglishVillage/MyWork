import com.google.gson.Gson;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.core.*;
import io.searchbox.core.search.sort.Sort;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by cube on 16-11-23.
 */
public class ESUtilsJest {
    public static void main(String[] args) throws Exception {
        createIndex("jestindex");

//        List<String> indexes = null;
//        List<String> types = null;
//        List<Sort> sorts = null;
//        QueryBuilder query = null;
//
//        indexes = new ArrayList<String>() {{
//            add("newkangkang");
//        }};
//        types = new ArrayList<String>() {{
//            add("newdata");
//        }};
//        query = QueryBuilders.rangeQuery("birthday").from("2015-03-23").to("2016-03-23T19:32:43").includeUpper(true).includeLower(true);   //yes
//
//        ESUtilsJest.operSearch(indexes, types, query, sorts, 1, 10, Model.class);
    }

    public static JestHttpClient getESClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                //.Builder("http://192.168.2.150:9200")
                .Builder("http://192.168.2.151:9200")
                .discoveryEnabled(true)//开启发现
                //.discoveryFrequency(500l, TimeUnit.MILLISECONDS)//发现频率
                //.clusterName("")//这个不用设置???
                .multiThreaded(true)
                //.maxTotalConnection(5)
                .build());
        JestHttpClient client = (JestHttpClient) factory.getObject();
        return client;
    }

    public static void closeClient(JestHttpClient client) {
        client.shutdownClient();
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午7:55
     * 名称：判断indexes是否存在
     * 备注：
     */
    public static boolean isExistIndex(String... indexes) {
        JestHttpClient client = getESClient();
        try {
            String INDEX_NAME = "it_typexst_0";
            String EXISTING_INDEX_TYPE = "ittypex";
            new IndexRequest(INDEX_NAME, EXISTING_INDEX_TYPE).source("{\"user\":\"tweety\"}").refresh(true);
            return false;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-23 上午10:12
     * 名称：创建空索引 默认setting 无mapping
     * 备注：5个分片，1个备份
     */
    public static boolean createIndex(String index) throws IOException {
        JestHttpClient client = getESClient();
        // 如果索引存在,删除索引
        DeleteIndex deleteIndex = new DeleteIndex.Builder(index).build();
        client.execute(deleteIndex);

        // 创建索引
        CreateIndex createIndex = new CreateIndex.Builder(index).build();
        JestResult result = client.execute(createIndex);
        return result.isSucceeded();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:46
     * 名称：给
     * 备注：如果index中已经有相同的字段，但是这次修改mapping跟上次类型不一致，则会报如下异常
     * java.lang.IllegalArgumentException:
     * mapper [name] of different type, current_type [string], merged_type [long]
     * mapping:mapping的json字符串
     */
    public static boolean createType(String index, String type, String mapping) throws IOException {
        JestHttpClient client = getESClient();
        PutMapping map = new PutMapping.Builder(index, type, mapping).build();
        JestResult result = client.execute(map);
        return result.isSucceeded();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午6:24
     * 名称：获取index下type的mapping信息
     * 备注：
     */
    public static String getMapping(List<String> indexes, List<String> types) throws IOException {
        JestHttpClient client = getESClient();
        GetMapping map = new GetMapping.Builder().addIndex(indexes).addType(types).build();
        JestResult result = client.execute(map);
        return result.getJsonString();
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/6 15:27
     * 名称：删除索引
     * 备注：
     */
    public static boolean operDelete(String index) throws IOException {
        JestHttpClient client = getESClient();
        DeleteIndex indicesExists = new DeleteIndex.Builder(index).build();
        JestResult result = client.execute(indicesExists);
        System.out.println(result.getErrorMessage() + result.isSucceeded());
        return result.isSucceeded();
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：删除
     * 备注：
     * id:主键，1.id存在情况下优先删除id
     * type：表，2.id不存在，优先删除type
     * index:数据库，3.type不存在，删除index
     */
    public static boolean operDelete(String index, String type, String id) throws IOException {
        JestHttpClient client = getESClient();
        Delete delete = new Delete.Builder(id)
                .index(index)
                .type(type)
                .build();
        JestResult result = client.execute(delete);
        return result.isSucceeded();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：删除
     * 备注：
     * id:主键，1.id存在情况下优先删除id
     * type：表，2.id不存在，优先删除type
     * index:数据库，3.type不存在，删除index
     */
    public static void operDeleteAsync(String index, String type, String id) throws IOException {
        JestHttpClient client = getESClient();
        Delete delete = new Delete.Builder(id).index(index).type(type).build();
        client.executeAsync(delete, new JestResultHandler<DocumentResult>() {
            @Override
            public void completed(DocumentResult result) {
                System.out.println(result.isSucceeded());
            }

            @Override
            public void failed(Exception ex) {
                ex.printStackTrace();
                System.err.println("failed during the asynchronous calling");
            }
        });
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：删除
     * 备注：
     * id:主键，1.id存在情况下优先删除id
     * type：表，2.id不存在，优先删除type
     * index:数据库，3.type不存在，删除index
     */
    public static boolean operDelete(String index, String type) throws IOException {
        return operDelete(index, type, "{\"query\": {\"match_all\": {}}}");
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：删除
     * 备注：
     * id:主键，1.id存在情况下优先删除id
     * type：表，2.id不存在，优先删除type
     * index:数据库，3.type不存在，删除index
     */
    public static boolean operDelete(String index, String type, QueryBuilder query) throws IOException {
        JestHttpClient client = getESClient();
        DeleteByQuery deleteByQuery = new DeleteByQuery.Builder(query.toString())
                .addIndex(index)
                .addType(type)
                .build();
        JestResult result = client.execute(deleteByQuery);
        return result.isSucceeded();
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:33
     * 名称：添加/修改
     * 备注：
     * index:数据库
     * type：表
     * id:主键【不指定则自动生成，自动生成则无法修改】
     * version:版本号，修改的时候加上version，避免覆盖掉新的数据。.【如果小于1则不设置版本号】
     * source：数据源【不可以为空，对象个数至少一个或者为偶数个。】
     */
    public static boolean operIndex(String index, String type, String id, long currVersion, String parent, Object source) throws IOException {
        if (source == null) {
            System.err.println("插入数据不可以为可空！");
            return false;
        } else {
            JestHttpClient client = getESClient();
            Index.Builder builder = new Index.Builder(source);
            //builder.setHeader(PWDKEY, getSecret());
            builder.id(id);
            builder.refresh(true);
            Index indexBuild = builder.index(index).type(type).build();
            JestResult result = client.execute(indexBuild);
            return result.isSucceeded();
        }
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午6:46
     * 名称：批处理
     * 备注：
     * lines:json字符串集合
     */
    public static <T extends Object> boolean operBulk(String index, String type, List<T> list) throws IOException {
        JestHttpClient client = getESClient();
        Bulk.Builder builder = new Bulk.Builder().defaultIndex(index).defaultType(type);
        for (T t : list) {
            //builder=builder.addAction(new Index.Builder(t).index(index).type(type).id("xxx").build());
            builder = builder.addAction(new Index.Builder(t).build());
        }
        Bulk bulk = builder.build();
        JestResult result = client.execute(bulk);
        return result.isSucceeded();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * query:查询结果之前的查询条件
     * filter:查询结果之后的过滤条件
     * sorts:排序字段及排序方式
     * pageIndex:页数[默认为1]
     * pageSize:页码[默认为10]
     */
    public static <T extends Object> List<T> operSearch(List<String> indexes, List<String> types, QueryBuilder query, List<Sort> sorts, int pageIndex, int pageSize, Class<T> tClass) throws IOException {
        JestHttpClient client = getESClient();
        Search search = new Search.Builder(query.toString())
                .addIndex(indexes)
                .addType(types)
                .addSort(sorts)
                .setParameter("from", (pageIndex - 1) * pageSize)
                .setParameter("size", pageSize)
                .build();
        JestResult result = client.execute(search);
        //result.getSourceAsObjectList()
        System.out.println(result.getJsonString());
        return result.getSourceAsObjectList(tClass);
    }


//    static final String PWDKEY = "X-SCE-ES-PASSWORD";
//    protected static String getSecret() {
//        long time = System.currentTimeMillis() / 1000;
//        return time + "," + DigestUtils.md5Hex(time).toUpperCase();
//    }

    private String getQueryString(QueryBuilder query) {
        return "{\"query\":" + query.toString() + "}";
    }

}
