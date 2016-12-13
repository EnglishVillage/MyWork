import com.google.gson.Gson;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import solutions.siren.join.SirenJoinPlugin;
import solutions.siren.join.action.coordinate.CoordinateSearchRequestBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static solutions.siren.join.index.query.QueryBuilders.filterJoin;

/**
 * Created by cube on 16-11-23.
 */
public class ESUtilsNew {
    String index = "testcreateindex";
    String type = "testtype";

    @Test
    public void mytest() {
//        ESUtilsNew.operDelete("iteblog_book_index",null,null);
//        ESUtilsNew.operDelete("newkangkang",null,null);
//        ESUtilsNew.operDelete("testcreateindex",null,null);

        //ESUtilsNew.operDelete("base_new", "yymf_drugs_cn", null);

        index = "newkangkangaaa";
        type = "newdata";
//        QueryBuilder query = QueryBuilders.matchAllQuery();
//        QueryBuilder filter = null;
//        SortBuilder[] sorts = {
//                //SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
//                //SortBuilders.fieldSort("transxm").order(SortOrder.DESC),
//                SortBuilders.fieldSort("custid").order(SortOrder.ASC)
//        };


        ESUtilsNew.operDelete("discoverdrugs2","discoverdrugs",null);
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:35
     * 名称：创建/判断/删除index
     * 备注：
     */
    @Test
    public void testcreateIndex() {
        System.out.println("创建空index'" + index + "':" + ESUtilsNew.createIndex(index, 3, 3));
        System.out.println("空index'" + index + "'存在：" + ESUtilsNew.isExistIndex(index));
        System.out.println("空index'" + index + "'删除成功：" + ESUtilsNew.operDelete(index, null, null));
        System.out.println("创建空index'testcreateindex':" + ESUtilsNew.createIndex(index));
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:37
     * 名称：给index添加mapping【不推荐，建议使用插件或者DSL方式修改】
     * 备注：
     */
    @Test
    public void testopercreateType() throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("properties")
                .startObject("name").field("type", "string").field("store", "yes").endObject()
                .startObject("age").field("type", "long").field("store", "yes").endObject()
                .startObject("birthday").field("type", "date").field("store", "yes").endObject()
                .endObject()
                .endObject()
                .endObject();

        boolean b = ESUtilsNew.createType(index, type, mappingBuilder);
        System.out.println("给index'" + index + "'添加mapping:" + b);
        System.out.println(ESUtilsNew.getMapping(index, type));

        try {
            mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(type)
                    .startObject("properties")
                    .startObject("name").field("type", "string").field("store", "yes").endObject()
                    .startObject("age").field("type", "long").field("store", "no").endObject()
                    .startObject("birthday").field("type", "date").field("store", "yes").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            b = ESUtilsNew.createType(index, type, mappingBuilder);
            System.out.println("给index'" + index + "'添加mapping:" + b);
        } catch (Exception e) {
            System.err.println("如果index中已经有相同的字段，但是这次修改mapping跟上次类型不一致，则会报如下异常");
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午6:11
     * 名称：增删改操作
     * 备注：
     */
    @Test
    public void testoperCRUD() {
        String id = "1";
        String id2 = "2";
        String id3 = "3";
        String parent = null;
        Object source = new Person("aaa", 111, "2016-01-01", new ArrayList<Book>() {{
            add(new Book("storm", 919));
        }});
        Object source2 = new Person("bbb", 222, "2016-03-23T19:32:43", new ArrayList<Book>() {{
            add(new Book("hadoop", 919));
        }});


        ESUtilsNew.operIndex(index, type, id, 0, parent, source);//新增：自己指定id
        ESUtilsNew.operIndex(index, type, null, 0, parent, source2);//新增：id自动生产
        ESUtilsNew.operIndex(index, type, id, 0, parent, source2);//修改：必须确定id
        ESUtilsNew.operIndex(index, type, id2, 0, parent, source2);//修改：必须确定id
        ESUtilsNew.operIndex(index, type, id3, 0, parent, source);//修改：必须确定id
        Map<String, Object> map = ESUtilsNew.operGet(index, type, id3);
        List<Map<String, Object>> list = ESUtilsNew.operGetMulti(index, type, id, id2, id3);
        System.out.println("id:'" + id3 + "'删除成功：" + ESUtilsNew.operDelete(index, type, id3));
        //1.修改字段值
        ESUtilsNew.operGet(index, type, id2);
        HashMap<String, Object> updateMap = new HashMap<String, Object>() {{
            put("birthday", "2015-03-23T19:32:43");
        }};
        System.out.println("1.修改字段值");
        ESUtilsNew.operUpdate(index, type, id, updateMap);
        ESUtilsNew.operGet(index, type, id2);
        //1.如果字段不存在，则添加字段
        updateMap.put("newField", "newValue");
        System.out.println("1.如果字段不存在，则添加字段");
        ESUtilsNew.operUpdate(index, type, id2, updateMap);
        ESUtilsNew.operGet(index, type, id2);
        //2.使用脚本方式：修改字段值
        //new Script("ctx._source.gender = \"male\"")   格式：【ctx.source.属性名=XXXX】
        System.out.println("2.使用脚本方式：修改字段值");
        ESUtilsNew.operUpdate(index, type, id2, new Script("ctx._source.birthday = \"2015-08-23T19:32:43\""));
        ESUtilsNew.operGet(index, type, id2);
        //2.使用脚本方式：如果字段不存在，则添加字段
        System.out.println("2.如果字段不存在，则添加字段");
        ESUtilsNew.operUpdate(index, type, id2, new Script("ctx._source.gender = \"male\""));
        ESUtilsNew.operGet(index, type, id2);
        //2.删除这个id的字段，并不会删除其他id的字段
        System.out.println("2.删除这个id的字段，并不会删除其他id的字段");
        ESUtilsNew.operUpdate(index, type, id2, new Script("ctx._source.remove(\"gender\")"));
        ESUtilsNew.operGet(index, type, id2);
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-24 上午11:32
     * 名称：查询案例1
     * 备注：
     */
    @Test
    public void testSearch1() {
        index = "newkangkang";
        type = "newdata";

        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        SortBuilder[] sorts = {
                //SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
                //SortBuilders.fieldSort("transxm").order(SortOrder.DESC),
                SortBuilders.fieldSort("custid").order(SortOrder.ASC)
        };

//        //批处理导入数据
//        ESUtilsNew.operBulk(index, type, Arrays.asList(getJsonArr()));
//        //新增修改数据，有1s延迟，刷新会马上查询到
//        System.out.println("刷新成功：" + ESUtilsNew.refresh(index));
//        SearchResponse response = ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//
//        System.out.println(ESUtilsNew.getMapping(index, type));
//        //字段查询
//        query = QueryBuilders.termQuery("merchants", "Harvey Nichols");//no   【string默认有分词】
//        //filter= QueryBuilders.termQuery("merchants", "aaa");//no   【string默认有分词】
//        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.matchQuery("merchants", "Harvey Nichols");//yes
//        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.termQuery("merchants", "chols");//no   【string默认有分词】
//        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.matchQuery("merchants", "chols");//no     【根据空格分词】
//        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.termQuery("atmFrequency", 10);     //yes          【long默认有分词】
//        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
//        query = QueryBuilders.matchQuery("atmFrequency", 10);     //yes
//        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);

        //范围查询【左右都闭合】
        query = QueryBuilders.rangeQuery("custid").from(10011).to(10015);   //yes
        String s = query.toString();
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 20);
        query = QueryBuilders.rangeQuery("birthday").from("2015-03-23").to("2016-03-23T19:32:43").includeUpper(true).includeLower(true);   //yes
        ESUtilsNew.operSearch(new String[]{"testcreateindex"}, new String[]{"testtype"}, query, filter, null, 0, 20);


        //批次查询
//        QueryBuilder[] querys = {
//                QueryBuilders.rangeQuery("custid").from(10011).to(10015),
//                QueryBuilders.termQuery("atmFrequency", 10),
//                QueryBuilders.termQuery("transxm",17)
//        };
//        ESUtilsNew.operSearchMulti(new String[]{index}, null, querys, filter, 0, 0);
//        ESUtilsNew.operSearchScroll(new String[]{index}, new String[]{type}, query, filter, sort,30000,15);
//        ESUtilsNew.operSearchTerminate(new String[]{index}, new String[]{type}, query, filter,7);//20条数据，<7才有效果
    }

    private String[] getJsonArr() {
        String[] jsonArr =
                {
                        "{\"custid\":10007,\"ntranstime\":1475437449785,\"transamt\":10222.14,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10222.14,\"atmTime\":1475437449785,\"atmAmount\":3923.15,\"atmFrequency\":6,\"atmCurrentPersonAmt\":3923.15}",
                        "{\"custid\":10010,\"ntranstime\":1475437446779,\"transamt\":8305.7705,\"merchants\":\"Carrefour\",\"transxm\":10,\"CurrentPersonAmt\":8305.7705,\"atmTime\":1475437416723,\"atmAmount\":5747.5996,\"atmFrequency\":6,\"atmCurrentPersonAmt\":5747.5996}",
                        "{\"custid\":10001,\"ntranstime\":1475437442772,\"transamt\":3490.9697,\"merchants\":\"La Rlnascente\",\"transxm\":5,\"CurrentPersonAmt\":3490.9697,\"atmTime\":1475437438764,\"atmAmount\":6408.8696,\"atmFrequency\":10,\"atmCurrentPersonAmt\":6408.8696}",
                        "{\"custid\":10003,\"ntranstime\":1475437361618,\"transamt\":8757.19,\"merchants\":\"Carrefour\",\"transxm\":14,\"CurrentPersonAmt\":8757.19,\"atmTime\":1475437411714,\"atmAmount\":11501.74,\"atmFrequency\":15,\"atmCurrentPersonAmt\":11501.74}",
                        "{\"custid\":10018,\"ntranstime\":1475437435758,\"transamt\":10456.45,\"merchants\":\"Lafayette\",\"transxm\":15,\"CurrentPersonAmt\":10456.45,\"atmTime\":1475437360616,\"atmAmount\":6614.98,\"atmFrequency\":13,\"atmCurrentPersonAmt\":6614.98}",
                        "{\"custid\":10016,\"ntranstime\":1475437424737,\"transamt\":13565.02,\"merchants\":\"Macy's\",\"transxm\":17,\"CurrentPersonAmt\":13565.02,\"atmTime\":1475437447781,\"atmAmount\":9488.27,\"atmFrequency\":13,\"atmCurrentPersonAmt\":9488.27}",
                        "{\"custid\":10009,\"ntranstime\":1475437420730,\"transamt\":4988.57,\"merchants\":\"Macy's\",\"transxm\":9,\"CurrentPersonAmt\":4988.57,\"atmTime\":1475437431750,\"atmAmount\":7295.47,\"atmFrequency\":9,\"atmCurrentPersonAmt\":7295.47}",
                        "{\"custid\":10005,\"ntranstime\":1475437396684,\"transamt\":13259.92,\"merchants\":\"Harrods\",\"transxm\":17,\"CurrentPersonAmt\":13259.92,\"atmTime\":1475437452791,\"atmAmount\":12974.68,\"atmFrequency\":16,\"atmCurrentPersonAmt\":12974.68}",
                        "{\"custid\":10012,\"ntranstime\":1475437440768,\"transamt\":8120.5903,\"merchants\":\"Macy's\",\"transxm\":10,\"CurrentPersonAmt\":8120.5903,\"atmTime\":1475437374643,\"atmAmount\":11250.45,\"atmFrequency\":13,\"atmCurrentPersonAmt\":11250.45}",
                        "{\"custid\":10014,\"ntranstime\":1475437410712,\"transamt\":10662.631,\"merchants\":\"Lafayette\",\"transxm\":11,\"CurrentPersonAmt\":10662.631,\"atmTime\":1475437339566,\"atmAmount\":7478.9707,\"atmFrequency\":7,\"atmCurrentPersonAmt\":7478.9707}",
                        "{\"custid\":10013,\"ntranstime\":1475437398688,\"transamt\":10226.88,\"merchants\":\"Harvey Nichols\",\"transxm\":15,\"CurrentPersonAmt\":10226.88,\"atmTime\":1475437331550,\"atmAmount\":7114.0005,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7114.0005}",
                        "{\"custid\":10017,\"ntranstime\":1475437450787,\"transamt\":11638.35,\"merchants\":\"Harvey Nichols\",\"transxm\":16,\"CurrentPersonAmt\":11638.35,\"atmTime\":1475437450787,\"atmAmount\":13887.99,\"atmFrequency\":19,\"atmCurrentPersonAmt\":13887.99}",
                        "{\"custid\":10019,\"ntranstime\":1475437391676,\"transamt\":7576.5703,\"merchants\":\"Harvey Nichols\",\"transxm\":11,\"CurrentPersonAmt\":7576.5703,\"atmTime\":1475437207311,\"atmAmount\":10627.9,\"atmFrequency\":11,\"atmCurrentPersonAmt\":10627.9}",
                        "{\"custid\":10015,\"ntranstime\":1475437422734,\"transamt\":14699.88,\"merchants\":\"Harvey Nichols\",\"transxm\":17,\"CurrentPersonAmt\":14699.88,\"atmTime\":1475437422734,\"atmAmount\":11425.541,\"atmFrequency\":11,\"atmCurrentPersonAmt\":11425.541}",
                        "{\"custid\":10008,\"ntranstime\":1475437412716,\"transamt\":4864.04,\"merchants\":\"Harvey Nichols\",\"transxm\":7,\"CurrentPersonAmt\":4864.04,\"atmTime\":1475437289468,\"atmAmount\":7952.57,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7952.57}",
                        "{\"custid\":10020,\"ntranstime\":1475437433754,\"transamt\":8433.239,\"merchants\":\"Lafayette\",\"transxm\":12,\"CurrentPersonAmt\":8433.239,\"atmTime\":1475437428744,\"atmAmount\":10674.0205,\"atmFrequency\":13,\"atmCurrentPersonAmt\":10674.0205}",
                        "{\"custid\":10006,\"ntranstime\":1475437437762,\"transamt\":12451.201,\"merchants\":\"Carrefour\",\"transxm\":18,\"CurrentPersonAmt\":12451.201,\"atmTime\":1475437342572,\"atmAmount\":5672.7197,\"atmFrequency\":9,\"atmCurrentPersonAmt\":5672.7197}",
                        "{\"custid\":10004,\"ntranstime\":1475437444776,\"transamt\":11164.301,\"merchants\":\"La Rlnascente\",\"transxm\":15,\"CurrentPersonAmt\":11164.301,\"atmTime\":1475437358612,\"atmAmount\":5574.6196,\"atmFrequency\":10,\"atmCurrentPersonAmt\":5574.6196}",
                        "{\"custid\":10011,\"ntranstime\":1475437400693,\"transamt\":14450.76,\"merchants\":\"Lafayette\",\"transxm\":17,\"CurrentPersonAmt\":14450.76,\"atmTime\":1475437389672,\"atmAmount\":9020.359,\"atmFrequency\":10,\"atmCurrentPersonAmt\":9020.359}",
                        "{\"custid\":10002,\"ntranstime\":1475437445778,\"transamt\":10803.89,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10803.89,\"atmTime\":1475437451789,\"atmAmount\":8754.38,\"atmFrequency\":11,\"atmCurrentPersonAmt\":8754.38}"
                };
        return jsonArr;
    }

    private String[] getJsonArr2() {
        String[] jsonArr =
                {
                        "{\"_id\":\"wkz10007\",\"custid\":10007,\"ntranstime\":1475437449785,\"transamt\":10222.14,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10222.14,\"atmTime\":1475437449785,\"atmAmount\":3923.15,\"atmFrequency\":6,\"atmCurrentPersonAmt\":3923.15}",
                        "{\"_id\":\"wkz10010\",\"custid\":10010,\"ntranstime\":1475437446779,\"transamt\":8305.7705,\"merchants\":\"Carrefour\",\"transxm\":10,\"CurrentPersonAmt\":8305.7705,\"atmTime\":1475437416723,\"atmAmount\":5747.5996,\"atmFrequency\":6,\"atmCurrentPersonAmt\":5747.5996}",
                        "{\"_id\":\"wkz10001\",\"custid\":10001,\"ntranstime\":1475437442772,\"transamt\":3490.9697,\"merchants\":\"La Rlnascente\",\"transxm\":5,\"CurrentPersonAmt\":3490.9697,\"atmTime\":1475437438764,\"atmAmount\":6408.8696,\"atmFrequency\":10,\"atmCurrentPersonAmt\":6408.8696}",
                        "{\"_id\":\"wkz10003\",\"custid\":10003,\"ntranstime\":1475437361618,\"transamt\":8757.19,\"merchants\":\"Carrefour\",\"transxm\":14,\"CurrentPersonAmt\":8757.19,\"atmTime\":1475437411714,\"atmAmount\":11501.74,\"atmFrequency\":15,\"atmCurrentPersonAmt\":11501.74}",
                        "{\"_id\":\"wkz10018\",\"custid\":10018,\"ntranstime\":1475437435758,\"transamt\":10456.45,\"merchants\":\"Lafayette\",\"transxm\":15,\"CurrentPersonAmt\":10456.45,\"atmTime\":1475437360616,\"atmAmount\":6614.98,\"atmFrequency\":13,\"atmCurrentPersonAmt\":6614.98}",
                        "{\"_id\":\"wkz10016\",\"custid\":10016,\"ntranstime\":1475437424737,\"transamt\":13565.02,\"merchants\":\"Macy's\",\"transxm\":17,\"CurrentPersonAmt\":13565.02,\"atmTime\":1475437447781,\"atmAmount\":9488.27,\"atmFrequency\":13,\"atmCurrentPersonAmt\":9488.27}",
                        "{\"_id\":\"wkz10009\",\"custid\":10009,\"ntranstime\":1475437420730,\"transamt\":4988.57,\"merchants\":\"Macy's\",\"transxm\":9,\"CurrentPersonAmt\":4988.57,\"atmTime\":1475437431750,\"atmAmount\":7295.47,\"atmFrequency\":9,\"atmCurrentPersonAmt\":7295.47}",
                        "{\"_id\":\"wkz10005\",\"custid\":10005,\"ntranstime\":1475437396684,\"transamt\":13259.92,\"merchants\":\"Harrods\",\"transxm\":17,\"CurrentPersonAmt\":13259.92,\"atmTime\":1475437452791,\"atmAmount\":12974.68,\"atmFrequency\":16,\"atmCurrentPersonAmt\":12974.68}",
                        "{\"_id\":\"wkz10012\",\"custid\":10012,\"ntranstime\":1475437440768,\"transamt\":8120.5903,\"merchants\":\"Macy's\",\"transxm\":10,\"CurrentPersonAmt\":8120.5903,\"atmTime\":1475437374643,\"atmAmount\":11250.45,\"atmFrequency\":13,\"atmCurrentPersonAmt\":11250.45}",
                        "{\"_id\":\"wkz10014\",\"custid\":10014,\"ntranstime\":1475437410712,\"transamt\":10662.631,\"merchants\":\"Lafayette\",\"transxm\":11,\"CurrentPersonAmt\":10662.631,\"atmTime\":1475437339566,\"atmAmount\":7478.9707,\"atmFrequency\":7,\"atmCurrentPersonAmt\":7478.9707}",
                        "{\"_id\":\"wkz10013\",\"custid\":10013,\"ntranstime\":1475437398688,\"transamt\":10226.88,\"merchants\":\"Harvey Nichols\",\"transxm\":15,\"CurrentPersonAmt\":10226.88,\"atmTime\":1475437331550,\"atmAmount\":7114.0005,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7114.0005}",
                        "{\"_id\":\"wkz10017\",\"custid\":10017,\"ntranstime\":1475437450787,\"transamt\":11638.35,\"merchants\":\"Harvey Nichols\",\"transxm\":16,\"CurrentPersonAmt\":11638.35,\"atmTime\":1475437450787,\"atmAmount\":13887.99,\"atmFrequency\":19,\"atmCurrentPersonAmt\":13887.99}",
                        "{\"_id\":\"wkz10019\",\"custid\":10019,\"ntranstime\":1475437391676,\"transamt\":7576.5703,\"merchants\":\"Harvey Nichols\",\"transxm\":11,\"CurrentPersonAmt\":7576.5703,\"atmTime\":1475437207311,\"atmAmount\":10627.9,\"atmFrequency\":11,\"atmCurrentPersonAmt\":10627.9}",
                        "{\"_id\":\"wkz10015\",\"custid\":10015,\"ntranstime\":1475437422734,\"transamt\":14699.88,\"merchants\":\"Harvey Nichols\",\"transxm\":17,\"CurrentPersonAmt\":14699.88,\"atmTime\":1475437422734,\"atmAmount\":11425.541,\"atmFrequency\":11,\"atmCurrentPersonAmt\":11425.541}",
                        "{\"_id\":\"wkz10008\",\"custid\":10008,\"ntranstime\":1475437412716,\"transamt\":4864.04,\"merchants\":\"Harvey Nichols\",\"transxm\":7,\"CurrentPersonAmt\":4864.04,\"atmTime\":1475437289468,\"atmAmount\":7952.57,\"atmFrequency\":10,\"atmCurrentPersonAmt\":7952.57}",
                        "{\"_id\":\"wkz10020\",\"custid\":10020,\"ntranstime\":1475437433754,\"transamt\":8433.239,\"merchants\":\"Lafayette\",\"transxm\":12,\"CurrentPersonAmt\":8433.239,\"atmTime\":1475437428744,\"atmAmount\":10674.0205,\"atmFrequency\":13,\"atmCurrentPersonAmt\":10674.0205}",
                        "{\"_id\":\"wkz10006\",\"custid\":10006,\"ntranstime\":1475437437762,\"transamt\":12451.201,\"merchants\":\"Carrefour\",\"transxm\":18,\"CurrentPersonAmt\":12451.201,\"atmTime\":1475437342572,\"atmAmount\":5672.7197,\"atmFrequency\":9,\"atmCurrentPersonAmt\":5672.7197}",
                        "{\"_id\":\"wkz10004\",\"custid\":10004,\"ntranstime\":1475437444776,\"transamt\":11164.301,\"merchants\":\"La Rlnascente\",\"transxm\":15,\"CurrentPersonAmt\":11164.301,\"atmTime\":1475437358612,\"atmAmount\":5574.6196,\"atmFrequency\":10,\"atmCurrentPersonAmt\":5574.6196}",
                        "{\"_id\":\"wkz10011\",\"custid\":10011,\"ntranstime\":1475437400693,\"transamt\":14450.76,\"merchants\":\"Lafayette\",\"transxm\":17,\"CurrentPersonAmt\":14450.76,\"atmTime\":1475437389672,\"atmAmount\":9020.359,\"atmFrequency\":10,\"atmCurrentPersonAmt\":9020.359}",
                        "{\"_id\":\"wkz10002\",\"custid\":10002,\"ntranstime\":1475437445778,\"transamt\":10803.89,\"merchants\":\"La Rlnascente\",\"transxm\":11,\"CurrentPersonAmt\":10803.89,\"atmTime\":1475437451789,\"atmAmount\":8754.38,\"atmFrequency\":11,\"atmCurrentPersonAmt\":8754.38}"
                };
        return jsonArr;
    }

    /**
     * 查询案例2
     */
    @Test
    public void testSearchHuiYi() {
        /**
         数据源：先插入数据源
         curl -XPUT 'http://192.168.2.124:9200/iteblog_book_index' -d '{ "settings": { "number_of_shards": 1 }}'

         curl -XPOST 'http://192.168.2.124:9200/iteblog_book_index/book/_bulk' -d '
         { "index": { "_id": 1 }}
         { "title": "Elasticsearch: The Definitive Guide", "authors": ["clinton gormley", "zachary tong"], "summary" : "A distibuted real-time search and analytics engine", "publish_date" : "2015-02-07", "num_reviews": 20, "publisher": "oreilly" }
         { "index": { "_id": 2 }}
         { "title": "Taming Text: How to Find, Organize, and Manipulate It", "authors": ["grant ingersoll", "thomas morton", "drew farris"], "summary" : "organize text using approaches such as full-text search, proper name recognition, clustering, tagging, information extraction, and summarization", "publish_date" : "2013-01-24", "num_reviews": 12, "publisher": "manning" }
         { "index": { "_id": 3 }}
         { "title": "Elasticsearch in Action", "authors": ["radu gheorge", "matthew lee hinman", "roy russo"], "summary" : "build scalable search applications using Elasticsearch without having to do complex low-level programming or understand advanced data science algorithms", "publish_date" : "2015-12-03", "num_reviews": 18, "publisher": "manning" }
         { "index": { "_id": 4 }}
         { "title": "Solr in Action", "authors": ["trey grainger", "timothy potter"], "summary" : "Comprehensive guide to implementing a scalable search engine using Apache Solr", "publish_date" : "2014-04-05", "num_reviews": 23, "publisher": "manning" }'


         * */


        String index = "iteblog_book_index";
        String type = "book";
        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        SortBuilder[] sorts = null;


        //所有字段中搜索guid关键字，不区分大小写。
        query = QueryBuilders.multiMatchQuery("guide", "_all");
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //title字段中搜索“in action”,默认会对它分词进行搜索
        query = QueryBuilders.matchQuery("title", "in action");
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //title,summer字段中搜索,并讲summer字段权重设置为3
        query = QueryBuilders.multiMatchQuery("elasticsearch guide", "title", "summary^3");
        query = QueryBuilders.multiMatchQuery("elasticsearch guide").field("title").field("summary", 3.0f);
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //布尔查询（与或非）
        query = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchQuery("title", "elasticsearch"))
                        .should(QueryBuilders.matchQuery("title", "Solr")))
                .must(QueryBuilders.matchQuery("authors", "clinton gormely"))
                .mustNot(QueryBuilders.matchQuery("authors", "radu gheorge"));
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //模糊查询【fuzziness("AUTO")，自动设置编辑距离，不区分大小写】
        query = QueryBuilders.multiMatchQuery("comprihensiv guide", "title", "summmary").fuzziness("AUTO");
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //通配符查询(区分大小写)
        query = QueryBuilders.wildcardQuery("authors", "T*");
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //正则表达式查询(区分大小写)
        query = QueryBuilders.regexpQuery("authors", "t[a-z]*y");
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //匹配短语查询(查询不到数据？？？)
        query = QueryBuilders.multiMatchQuery("search engine", "title", "summmary").type(MultiMatchQueryBuilder.Type.PHRASE).slop(3);

        //匹配短语前缀查询(匹配短语前缀查询是有性能消耗的，所有使用之前需要小心。)
        query = QueryBuilders.matchPhrasePrefixQuery("summary", "search en").slop(3).maxExpansions(10);
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //例子中，我们运行了一个模糊搜索(fuzzy search)，搜索关键字是"saerch algorithm"，
        //并且作者包含grant ingersoll或者tom morton。并且搜索了所有的字段，其中summary字段的权重为2：
        query = QueryBuilders.queryStringQuery("(saerch~1 algorithm~1) AND (grant ingersoll) OR (tom morton)").field("_all").field("summary", 2.0f);
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //这个是queryString的另外版本，使用+/|/- 分别替换AND/OR/NOT，如果用输入了错误的查询，其直接忽略这种情况而不是抛出异常
        query = QueryBuilders.simpleQueryStringQuery("(saerch~1 algorithm~1) + (grant ingersoll)  | (tom morton)").field("_all").field("summary", 2.0f);
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        query = QueryBuilders.termQuery("publisher", "manning");
        sorts = new SortBuilder[]{
                SortBuilders.fieldSort("publish_date").order(SortOrder.DESC),
                SortBuilders.fieldSort("title").order(SortOrder.DESC)
        };
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        query = QueryBuilders.termsQuery("publisher", "oreilly", "packt");      //2个字段的是值是或的关系
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //范围查询：应用于日期，数字以及字符类型的字段。【左右都闭合】
        query = QueryBuilders.rangeQuery("publish_date").gte("2015-01-01").lte("2015-12-31");
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //过滤查询：在实际运用中，过滤器应该先被执行，这样可以减少需要查询的范围。而且，第一次使用fliter之后其将会被缓存，这样会对性能代理提升。
        //第1种方式：已弃用
        query = QueryBuilders.filteredQuery(QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary"),
                QueryBuilders.rangeQuery("num_reviews").gte(20));
        //第2种方式：
        query = QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary");
        filter = QueryBuilders.rangeQuery("num_reviews").gte(20);
        //第3种方式：
        query = QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary"))
                .filter(QueryBuilders.rangeQuery("num_reviews").gte(20));
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        query = QueryBuilders.filteredQuery(QueryBuilders.multiMatchQuery("elasticsearch", "title", "summary"),
                QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("num_reviews").gte("20"))
                        .mustNot(QueryBuilders.rangeQuery("publish_date").lte("2014-12-31"))
                        .must(QueryBuilders.termQuery("publisher", "oreilly")));
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。

        //在某些场景下，你可能想对某个特定字段设置一个因子(factor)，并通过这个因子计算某个文档的相关度(relevance score)。
        //这是典型地基于文档(document)的重要性来抬高其相关性的方式。
        //在下面例子中，我们想找到更受欢迎的图书(是通过图书的评论实现的)，并将其权重抬高，这里通过使用FieldValueFactor来实现。
        query = QueryBuilders.functionScoreQuery(QueryBuilders.multiMatchQuery("search engine", "title", "summary"))
                .add(ScoreFunctionBuilders.fieldValueFactorFunction("num_reviews").factor(2.0f).modifier(FieldValueFactorFunction.Modifier.LOG1P));
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。


        //参考：https://www.iteblog.com/archives/1768
        //gauss 衰减速度先慢后快再慢，exp 衰减速度先快后慢，lin 直线衰减，在0分外的值都是0分，如何选择取决于你想要你的score以什么速度衰减。
        //下面例子中我们搜索标题或者摘要中包含search engines的图书，并且希望图书的发行日期是在2014-06-15中心点范围内，如下：
        query = QueryBuilders.functionScoreQuery(QueryBuilders.multiMatchQuery("search engine", "title", "summary"))
                .add(ScoreFunctionBuilders.exponentialDecayFunction("publish_date", "2014-06-15", "30d").setOffset("7d"))//fieldName,origin,scale,offset
                .boostMode(CombineFunction.REPLACE);
        ESUtilsNew.operSearch(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);//只显示10条，默认为10条。
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-24 上午11:32
     * 名称：分组查询
     * 备注：
     */
    @Test
    public void testSearchAgg() {
        index = "newkangkang";
        type = "newdata";

        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        String[] aggNames = null;
        AbstractAggregationBuilder[] aggBuilders = null;
        SearchResponse response = null;

//        /**
//         * 根据时间进行分组
//         * */
//        aggNames = new String[]{"birthday"};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                //                AggregationBuilders.terms(aggNames[0]).field("merchants"),
//                AggregationBuilders.dateHistogram(aggNames[0]).field("birthday").interval(DateHistogramInterval.YEAR)
//                //                AggregationBuilders.avg(aggNames[1]).field("custid")
//                //                AggregationBuilders.terms("by_country").field("country").subAggregation(
//                //                        AggregationBuilders.dateHistogram("by_year").field("dateOfBirth").interval((DateHistogramInterval.YEAR)).subAggregation(
//                //                                AggregationBuilders.avg("avg_children").field("children")
//                //                        )
//                //                )
//        };
//        response = ESUtilsNew.operSearchAgg(new String[]{"testcreateindex"}, new String[]{"testtype"}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            System.out.println(i);
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            InternalHistogram agg = (InternalHistogram) aggregation;
//            List<InternalHistogram.Bucket> buckets = agg.getBuckets();
//            for (InternalHistogram.Bucket bucket : buckets) {
//                System.out.println("key:" + bucket.getKeyAsString() + "\tcount:" + bucket.getDocCount());
//                Aggregations aggregations = bucket.getAggregations();
//                ValueFormatter formatter = bucket.getFormatter();
//                boolean keyed = bucket.getKeyed();
//            }
//        }
        aggNames = new String[]{"atmFrequency"};
        aggBuilders = new AbstractAggregationBuilder[]{
//                AggregationBuilders.terms(aggNames[0]).field("atmFrequency")
                AggregationBuilders.cardinality(aggNames[0]).field(aggNames[0])
        };
        response = ESUtilsNew.operSearchAgg(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
        for (int i = 0; i < aggNames.length; i++) {
            System.out.println(i);
            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
            InternalCardinality car = (InternalCardinality) aggregation;
            System.out.println(car);

//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();                     // bucket key
//                long docCount = entry.getDocCount();                        // Doc count
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//            }
        }
//        key:10, doc_count:5
//        key:13, doc_count:4
//        key:11, doc_count:3
//        key:6, doc_count:2
//        key:9, doc_count:2
//        key:7, doc_count:1
//        key:15, doc_count:1
//        key:16, doc_count:1
//        key:19, doc_count:1

//
//
//        /**
//         * 常用聚合函数
//         * */
//        aggNames = new String[]{"transxm_Min", "atmFrequency_Max",};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                AggregationBuilders.min(aggNames[0]).field("transxm"),
//                AggregationBuilders.max(aggNames[1]).field("atmFrequency")
//                //AggregationBuilders.sum(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.avg(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.count(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.cardinality(aggNames[0]).field("atmFrequency")      //基数
//        };
//        response = ESUtilsNew.operSearchAgg(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            if (i == 0) {
//                Min agg = (Min) aggregation;
//                System.out.println(agg.getName() + ":" + agg.getValue());
//            } else {
//                Max agg = (Max) aggregation;
//                System.out.println(agg.getName() + ":" + agg.getValue());
//            }
//            //Sum agg = (Sum) aggregation;
//            //Avg agg = (Avg) aggregation;
//            //ValueCount agg = (ValueCount) aggregation;
//            //Cardinality agg = (Cardinality) aggregation;
//        }
//
//
//        /**
//         * 常用聚合函数和常用统计函数
//         * */
//        aggNames = new String[]{"atmFrequency_stats", "transxm_extendedStats"};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                AggregationBuilders.stats(aggNames[0]).field("atmFrequency"),
//                AggregationBuilders.extendedStats(aggNames[1]).field("transxm")
//                //AggregationBuilders.percentiles(aggNames[0]).field("atmFrequency")
//                //AggregationBuilders.percentiles(aggNames[0]).field("atmFrequency").percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0)//后面参数是指 抽取百分之多少 数据。
//                //AggregationBuilders.percentileRanks(aggNames[0]).field("atmFrequency").percentiles(1, 50, 75)
//                ////获取边界【经纬度】
//                //AggregationBuilders.geoBounds(aggNames[0]).field("merchants")
//        };
//        response = ESUtilsNew.operSearchAgg(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            if (i == 0) {
//                Stats stats = (Stats) aggregation;
//                System.out.println(stats.getName() +
//                        ":\tmin:" + stats.getMinAsString() +
//                        "\tmax:" + stats.getMaxAsString() +
//                        "\tsum:" + stats.getSumAsString() +
//                        "\tavg:" + stats.getAvgAsString() +
//                        "\tcount:" + stats.getCountAsString());
//            } else {
//                ExtendedStats extendedStats = response.getAggregations().get(aggNames[i]);
//                System.out.println(extendedStats.getName() +
//                        ":\tmin:" + extendedStats.getMinAsString() +
//                        "\tmax:" + extendedStats.getMaxAsString() +
//                        "\tsum:" + extendedStats.getSumAsString() +
//                        "\tavg:" + extendedStats.getAvgAsString() +
//                        "\tcount:" + extendedStats.getCountAsString() +
//                        "\tstdDeviation:" + extendedStats.getStdDeviationAsString() +  //标准差
//                        "\tsumOfSquares:" + extendedStats.getSumOfSquaresAsString() +  //平方和
//                        "\tvariance:" + extendedStats.getVarianceAsString());         //方差
//            }
//        }
//
//        /**
//         * 分组之后聚合操作(.size(3)是分组之后显示的条数)
//         * */
//        aggNames = new String[]{"atmFrequency_top", "atmFrequency_sum", "atmFrequency_avg", "atmFrequency_count"};
//        aggBuilders = new AbstractAggregationBuilder[]{
//                //分组取几条数据（默认是取3条数据:.setFrom(0).setSize(3)）
//                //AggregationBuilders.terms(aggNames[0]).field("atmFrequency").subAggregation(AggregationBuilders.topHits("top"))
//                AggregationBuilders.terms(aggNames[0]).field("atmFrequency").size(3).subAggregation(
//                        AggregationBuilders.topHits("top").setExplain(false).setFrom(0).setSize(3)),
//                //分组在求和，根据分组字段进行升序
//                AggregationBuilders.terms(aggNames[1]).field("atmFrequency").subAggregation(
//                        AggregationBuilders.sum("sum").field("transxm")
//                ).order(Terms.Order.term(true)),
//                //分组在求平均/最小值/最大值，根据平均值/最小值/最大值进行降序
//                AggregationBuilders.terms(aggNames[2]).field("atmFrequency").subAggregation(
//                        AggregationBuilders.avg("avg").field("transxm")
//                        //AggregationBuilders.min("min").field("transxm")
//                        //AggregationBuilders.max("max").field("transxm")
//                ).order(Terms.Order.aggregation("avg", true)),
//                //分组在求和，根据分组字段里'文档个数总数'进行升序
//                AggregationBuilders.terms(aggNames[3]).field("atmFrequency").order(Terms.Order.count(true))
//        };
//        response = ESUtilsNew.operSearchAgg(new String[]{index}, new String[]{type}, query, filter, aggBuilders);
//        for (int i = 0; i < aggNames.length; i++) {
//            System.out.println(i);
//            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();                     // bucket key
//                long docCount = entry.getDocCount();                        // Doc count
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                //求top
//                if (i == 0) {
//                    TopHits topHits = entry.getAggregations().get("top");
//                    for (SearchHit hit : topHits.getHits().getHits()) {
//                        System.out.println(" -> id:" + hit.getId() + ", _source" + hit.getSourceAsString());
//                    }
//                } else if (i == 1) {
//                    //分组求平均,和,最大值,最小值,文档个数不用再调用聚合
//                    Sum chiHit = entry.getAggregations().get("sum");
//                    System.out.println("sum:" + chiHit.getValue());
//                } else if (i == 2) {
//                    //分组求平均,和,最大值,最小值,文档个数不用再调用聚合
//                    Avg chiHit = entry.getAggregations().get("avg");
//                    //Min chiHit = entry.getAggregations().get("min");
//                    //Max chiHit = entry.getAggregations().get("max");
//                    System.out.println("avg:" + chiHit.getValue());
//                } else if (i == 3) {
//                    //分组求平均,和,最大值,最小值,文档个数不用再调用聚合
//                    System.out.println("count:" + entry.getDocCount());
//                }
//            }
//        }





        //分组再分组再统计
        //aggBuilders = new AbstractAggregationBuilder[]{
        //        AggregationBuilders.terms(aggNames[0]).field("merchants"),
        //        AggregationBuilders.terms("by_country").field("country").subAggregation(
        //                AggregationBuilders.dateHistogram("by_year").field("dateOfBirth").interval((DateHistogramInterval.YEAR)).subAggregation(
        //                        AggregationBuilders.avg("avg_children").field("children")
        //                )
        //        )
        //};
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-21 下午7:03
     * 名称：父子文档查询
     * 备注：
     * 插入数据的时候要先建立索引
     * 参考：http://blog.csdn.net/napoay/article/details/52032931
     */
    @Test
    public void testSearchParent() {
        /**
         * 数据源:

         curl -XPUT  "http://192.168.2.124:9200/parent" -d '
         {
         "mappings": {
         "branch": {},
         "employee": {
         "_parent": {
         "type": "branch"
         }
         }
         }
         }'

         //父文档
         curl -XPOST 'http://192.168.2.124:9200/parent/branch/_bulk' -d '
         { "index": { "_id": "london" }}
         { "name": "London Westminster", "city": "London", "country": "UK" }
         { "index": { "_id": "liverpool" }}
         { "name": "Liverpool Central", "city": "Liverpool", "country": "UK" }
         { "index": { "_id": "paris" }}
         { "name": "Champs Élysées", "city": "Paris", "country": "France" }
         '

         //子文档
         curl -XPUT "http://192.168.2.124:9200/parent/employee/1?parent=london&pretty" -d '
         {
             "name":  "Alice Smith",
             "dob":   "1970-10-24",
             "hobby": "hiking"
         }'

         curl -XPOST 'http://192.168.2.124:9200/parent/employee/_bulk' -d '
         { "index": { "_id": 2, "parent": "london" }}
         { "name": "Mark Thomas", "dob": "1982-05-16", "hobby": "diving" }
         { "index": { "_id": 3, "parent": "liverpool" }}
         { "name": "Barry Smith", "dob": "1979-04-01", "hobby": "hiking" }
         { "index": { "_id": 4, "parent": "paris" }}
         { "name": "Adrien Grand", "dob": "1987-05-11", "hobby": "horses" }
         '

         * **/


        String index = "parent";
        String parType = "branch";
        String chiType = "employee";
        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = null;
        //SortBuilder sort = SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
        SortBuilder[] sorts = null;


        //通过子文档查询条件 显示相关父文档
        //搜索含有1980年以后出生的employee的branch
        query = QueryBuilders.hasChildQuery("employee", QueryBuilders.rangeQuery("dob").gte("1980-01-01"));//【子文档type,子文档查询条件】
        ESUtilsNew.operSearch(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
        //查询name中含有“Alice Smith”的branch
        //定义里嵌套对象计算的分数与当前查询分数的处理方式，有avg,sum,max,min以及none。none就是不做任何处理，其他的看字面意思就好理解。
        query = QueryBuilders.hasChildQuery("employee", QueryBuilders.matchQuery("name", "Alice Smith")).scoreMode("avg");
        ESUtilsNew.operSearch(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
        //搜索最少含有2个employee的branch：
        query = QueryBuilders.hasChildQuery("employee", QueryBuilders.matchAllQuery()).minChildren(2);
        ESUtilsNew.operSearch(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.

        //通过父文档查询条件 显示相关子文档
        query = QueryBuilders.hasParentQuery("branch", QueryBuilders.matchQuery("country", "UK"));
        ESUtilsNew.operSearch(new String[]{index}, new String[]{chiType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
        //搜索有相同父id的子文档：搜索父id为"liverpool","paris"的employee：
        query = QueryBuilders.hasParentQuery("branch", QueryBuilders.idsQuery().ids("liverpool", "paris"));
        ESUtilsNew.operSearch(new String[]{index}, new String[]{chiType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.


        ////三级父子关系的嵌套搜索【事例，并无数据】
        //TermsLookupQueryBuilder three = QueryBuilders.termsLookupQuery("uuid").lookupIndex("user").lookupType("user").lookupId("5").lookupPath("uuids");
        //HasChildQueryBuilder two = QueryBuilders.hasChildQuery("graType",three);//用孙子type
        //HasChildQueryBuilder one = QueryBuilders.hasChildQuery("chiType", two);//用儿子type
        //OldESUtils.operSearch(new String[]{index}, new String[]{"parType"}, query, filter,fields, sorts, 0, 0);//用父type


        //********************************************************************
        //对关联的 child 文档进行聚合操作[父type分组之后在子type再分组]（参考：http://www.cnblogs.com/licongyu/p/5557693.html）
        //先分组父type的国家，在分组子type的嗜好
        //********************************************************************
        String[] aggNames = {"country"};
        AbstractAggregationBuilder[] aggBuilders = {
                AggregationBuilders.terms(aggNames[0]).field("country").subAggregation(                 //这里设置根据父表的哪个字段进行分组
                        AggregationBuilders.children("employee").childType("employee").subAggregation(      //这里设置父表的子表
                                AggregationBuilders.terms("hobby").field("hobby")                           //这里设置根据子表的哪个字段进行分组
                        )
                )

        };
        SearchResponse response = ESUtilsNew.operSearchAgg(new String[]{index}, new String[]{parType}, query, filter, aggBuilders);//这里设置父表type
        for (int i = 0; i < aggNames.length; i++) {
            System.out.println(i);
            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
            //********************************************************************
            //父子表中：先根据父表的country分组，在设置子表，在根据子表的hobby进行分组。
            //********************************************************************
            Terms terms = (Terms) aggregation;
            for (Terms.Bucket entry : terms.getBuckets()) {
                String key = entry.getKeyAsString();
                long docCount = entry.getDocCount();
                System.out.println("key:" + key + ", doc_count:" + docCount);
                Children children = entry.getAggregations().get("employee");
                Terms chiTerms = children.getAggregations().get("hobby");
                for (Terms.Bucket chiEntry : chiTerms.getBuckets()) {
                    key = chiEntry.getKeyAsString();
                    docCount = chiEntry.getDocCount();
                    System.out.println("\t--> key:" + key + ", doc_count:" + docCount);
                }
            }
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/6 11:35
     * 名称：关联查询[跟父子文档查询类似,但不一样,它这个不是父子关系]
     * 备注：
     */
    @Test
    public void testSearchSiren() {
        /**
         * 数据源:
         curl -XPUT 'http://192.168.2.124:9200/_bulk?pretty' -d '
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "1" } }
         { "title" : "The NoSQL database glut", "mentions" : ["1", "2"] }
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "2" } }
         { "title" : "Graph Databases Seen Connecting the Dots", "mentions" : [] }
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "3" } }
         { "title" : "How to determine which NoSQL DBMS best fits your needs", "mentions" : ["2", "4"] }
         { "index" : { "_index" : "articles", "_type" : "article", "_id" : "4" } }
         { "title" : "MapR ships Apache Drill", "mentions" : ["4"] }

         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "1" } }
         { "id": "1", "name" : "Elastic" }
         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "2" } }
         { "id": "2", "name" : "Orient Technologies" }
         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "3" } }
         { "id": "3", "name" : "Cloudera" }
         { "index" : { "_index" : "companies", "_type" : "company", "_id" : "4" } }
         { "id": "4", "name" : "MapR" }
         '

         * **/

        index = "companies";
        type = "company";
        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder filter = QueryBuilders.matchAllQuery();
        SortBuilder[] sorts = null;
        filter = QueryBuilders.boolQuery().filter(
                //id属于company,mentions属于article;id和mentions是相关联的
                filterJoin("id").indices("articles").types("article").path("mentions").query(
                        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("title", "nosql"))
                )
                .orderBy("default")//只有2种排序方式:default,doc_score
                .maxTermsPerShard(5)//每个分片存储最多分组数量
        );
        //下面这个是简洁写法
        //filter=filterJoin("id").indices("articles").types("article").path("mentions").query(
        //        QueryBuilders.termQuery("title", "nosql")
        //);
        //查询出来只有type的所有字段
        ESUtilsNew.operSearchSiren(new String[]{index}, new String[]{type}, query, filter, sorts, 0, 0);


//        index = "parent";
//        String parType = "branch";
//        String chiType = "employee";
//        ////通过子文档查询条件 显示相关父文档
//        ////搜索含有1980年以后出生的employee的branch
//        //query = QueryBuilders.hasChildQuery("employee", QueryBuilders.rangeQuery("dob").gte("1980-01-01"));//【子文档type,子文档查询条件】
//        //ESUtilsNew.operSearch(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);//只用父type,不可以写子type.
//        //将上面转化为siren形式查询,转化失败.父子表查询是根据父表的_id来关联的.siren方式则不允许!!!
//        filter=QueryBuilders.boolQuery().filter(filterJoin("_id").indices(index).types(chiType).path("parent").query(
//                QueryBuilders.rangeQuery("dob").gte("1980-01-01")
//        ));
//        ESUtilsNew.operSearchSiren(new String[]{index}, new String[]{parType}, query, filter, sorts, 0, 0);
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午6:24
     * 名称：获取index下type的mapping信息
     * 备注：
     */@Deprecated
    public static String getMapping(String index, String type) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().get()
                    .getState().getMetaData().getIndices().get(index).getMappings();
            String s = mappings.get(type).source().toString();
            return s;
        } catch (IndexNotFoundException ex) {
            System.err.println("index 不存在！");
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午6:54
     * 名称：新增/修改的时候，refresh一下可以马上查询到。
     * 备注：每次新增/修改使用对性能有很大的影响
     */
    public static boolean refresh(String... index) {
        Client client = ESClientHelper.getInstance().getClient();
        RefreshResponse response = client.admin().indices().prepareRefresh().get();
        return response.isContextEmpty();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午7:55
     * 名称：判断indexes是否存在
     * 备注：
     */
    public static boolean isExistIndex(String... indexes) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            IndicesExistsResponse indicesExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(indexes)).get();
            return indicesExistsResponse.isExists();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午7:55
     * 名称：判断typees是否存在
     * 备注：
     */
    public static boolean isExistType(String[] indexes, String[] type) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            TypesExistsResponse typeResponse = client.admin().indices().prepareTypesExists(indexes).setTypes(type).get();
            return typeResponse.isExists();
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
     * 索引存在会抛出异常：IndexAlreadyExistsException
     */@Deprecated
    public static boolean createIndex(String index) {
        try {
            Client client = ESClientHelper.getInstance().getClient();
            CreateIndexResponse response = client.admin().indices().prepareCreate(index).get();
            return response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 上午10:12
     * 名称：创建空索引 默认setting 无mapping
     * 备注：索引存在会抛出异常：IndexAlreadyExistsException
     * shardNum:分片数（默认5）
     * replicaNum：备份数（默认1）
     */
    public static boolean createIndex(String index, int shardNum, int replicaNum) {
        try {
            Client client = ESClientHelper.getInstance().getClient();
            CreateIndexResponse response = client.admin().indices().prepareCreate(index)
                    .setSettings(Settings.builder().put("index.number_of_shards", shardNum).put("index.number_of_replicas", replicaNum).build())
                    .get();
            return response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:46
     * 名称：给
     * 备注：如果index中已经有相同的字段，但是这次修改mapping跟上次类型不一致，则会报如下异常
     * java.lang.IllegalArgumentException:
     * mapper [name] of different type, current_type [string], merged_type [long]
     */@Deprecated
    public static boolean createType(String index, String type, XContentBuilder mapping) {
        Client client = ESClientHelper.getInstance().getClient();
        PutMappingResponse response = client.admin().indices()
                .preparePutMapping(index)
                .setType(type)
                .setSource(mapping)
                .get();
        return response.isAcknowledged();
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：删除
     * 备注：
     * id:主键，1.id存在情况下优先删除id
     * type：表，2.id不存在，优先删除type
     * index:数据库，3.type不存在，删除index
     */@Deprecated
    public static boolean operDelete(String index, String type, String id) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            if (id != null) {
                DeleteResponse response = client.prepareDelete(index, type, id)
                        .get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
                System.out.println(response.getIndex() + "-" + response.getType() + "-" + response.getId() + "[" + response.getVersion() + "]" + ":" + response.isFound());
                return response.isFound();
            } else {
                if (type != null) {
                    if (index != null) {
                        //先判断type是否存在
                        TypesExistsResponse typeResponse = client.admin().indices().prepareTypesExists(index).setTypes(type).get();
                        if (typeResponse.isExists()) {
                            DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                                    .setIndices(index).setTypes(type)
                                    .setSource("{\"query\": {\"match_all\": {}}}")
                                    .get();
                            return response.isContextEmpty();
                        }
                    }
                } else {
                    if (index != null) {
                        //先判断index是否存在
                        IndicesAdminClient indices = client.admin().indices();
                        IndicesExistsResponse indicesExistsResponse = indices.exists(new IndicesExistsRequest(index)).get();
                        if (indicesExistsResponse.isExists()) {
                            DeleteIndexResponse deleteIndexResponse = indices.prepareDelete(index).get();
                            return deleteIndexResponse.isAcknowledged();
                        }
                    }
                }
                return false;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
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
     */@Deprecated
    public static boolean operIndex(String index, String type, String id, long currVersion, String parent, Object source) {
        if (source == null) {
            System.err.println("插入数据不可以为可空！");
            return false;
        } else {
            //转化为json字符串
            String jsonArr = ObjectToJson(source);
            Client client = ESClientHelper.getInstance().getClient();
            //初始化Request，传入index和type参数
            IndexRequestBuilder request = client.prepareIndex(index, type);
            //传入id参数
            if (id != null) {
                request = request.setId(id);//必须为对象单独指定ID,不指定则自动生成
            } else {
                //id为空，则自动生成
            }
            //传入parent参数
            if (parent != null) {
                request = request.setParent(parent);
            } else {
                //parent为空，则自动生成
            }
            //传入当前要操作的文档的版本号，一定要对应，不然修改失败。
            if (currVersion > 0) {
                request = request.setVersion(currVersion);
            }
            request = request.setSource(jsonArr);
            try {
                //执行操作，并返回操作结果,底层调用//.execute().actionGet();
                IndexResponse response = request.get();
                //多次index这个版本号会变
                //System.out.println("index response.version():" + response.getVersion());
                //response.isCreated();//只返回新增是否成功
                //返回新增或者修改是否成功
                return response.isContextEmpty();
            } catch (VersionConflictEngineException ex) {
                System.err.println("修改操作，请确定当前要更新的文档的版本号。添加操作，请将版本号设置为0。");
                return false;
            }
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取一个
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * id:主键
     */
    public static Map<String, Object> operGet(String index, String type, String id) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            GetResponse response = client.prepareGet(index, type, id).get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
            return printGetResponse(response);
        } catch (IndexNotFoundException ex) {
            System.err.println("index 不存在！");
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：根据id数组获取多个
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * ids:主键
     */
    public static List<Map<String, Object>> operGetMulti(String index, String type, String... ids) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            MultiGetResponse responses = client.prepareMultiGet()
                    .add(index, type, ids)
                    .get();
            return printGetResponses(responses);
        } catch (IndexNotFoundException ex) {
            System.err.println("index 不存在！");
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：修改文档或新增字段
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * id:主键，不存在不会报错
     * fieldValue:field存在则修改字段值，field不存在则添加该字段和值
     */
    public static boolean operUpdate(String index, String type, String id, Map<String, Object> fieldValue) {
        if (fieldValue == null || fieldValue.size() < 1) {
            System.err.println("要修改的Map不可以为空！");
            return false;
        }
        Client client = ESClientHelper.getInstance().getClient();
        try {
            //创建要修改的doc文档
            XContentBuilder doc = jsonBuilder().startObject();
            for (String key : fieldValue.keySet()) {
                doc = doc.field(key, fieldValue.get(key));
            }
            doc.endObject();

//            //只能修改不能添加（添加会报错）
////          第1种方式
//            UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(doc).get();
//            第2种方式
//            UpdateRequest request = new UpdateRequest(index, type, id);
//            request = request.doc(request);
//            UpdateResponse response = client.update(request).get();


            //可修改亦可添加（和client.prepareIndex(...)基本类似，但是感觉很鸡肋。）
            IndexRequest request = new IndexRequest(index, type, id);
            //封装成一个update请求
            request = request.source(doc);
            //创建insert请求
            UpdateRequest request2 = new UpdateRequest(index, type, id);
            //封装成一个insert请求
            String firstKey = fieldValue.keySet().toArray(new String[]{})[0];
            request2 = request2.doc(jsonBuilder().startObject().field(firstKey, fieldValue.get(firstKey)).endObject()).upsert(request);
            UpdateResponse response = client.update(request2).get();

            return response.isContextEmpty();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：更新【这个update方法柑橘不是那么好用，直接用operIndex操作】
     * 备注：3个参数一个都不能少。
     * index:数据库【不存在会报错】
     * type：表，不存在不会报错
     * id:主键，不存在不会报错
     * script:new Script("ctx._source.gender = \"male\"")   格式：【ctx.source.属性名=XXXX】
     * 里面的语法是groovy写的。
     */
    public static boolean operUpdate(String index, String type, String id, Script script) {
        Client client = ESClientHelper.getInstance().getClient();
        UpdateRequest request = new UpdateRequest(index, type, id);
        try {
            request = request.script(script);
            UpdateResponse response = client.update(request).get();
            return response.isContextEmpty();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-22 下午6:46
     * 名称：批处理
     * 备注：
     * lines:json字符串集合
     */@Deprecated
    public static boolean operBulk(String index, String type, List<String> lines) {
        Client client = ESClientHelper.getInstance().getClient();
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (String line : lines) {
                bulkRequest.add(client.prepareIndex(index, type).setSource(line));
            }
            BulkResponse response = bulkRequest.get();
            return response.isContextEmpty();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
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
    public static SearchResponse operSearch(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();

        SearchRequestBuilder request = getRequest(client, indexes, types, query, filter);
        //添加要显示的字段【这个暂时不知道怎么使用。】
        //request = getRequestByFields(request, fields);
        //设置高亮显示【暂时不使用】
        //request=request.addHighlightedField("tagname")
        //.setHighlighterEncoder("UTF-8")
        //.setHighlighterPreTags("<em>")
        //.setHighlighterPostTags("</em>");
        //这个暂时不知道怎么使用
        //request = request.setIndicesOptions(IndicesOptions.strictExpand());
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request = getRequestByPage(request, pageIndex, pageSize);
        //设置显示的字段（失败）
//        ArrayList<String> showFields = new ArrayList<String>(){{
//            add("summary");
//            add("authors");
//        }};
//        String[] showFields={"publish_date","num_reviews"};
//        HashMap<String, Object> sou = new HashMap<String, Object>(){{
//            put("include",showFields);
//        }};
//        request=request.setSource(sou);
        //说明
        //request = request.setExplain(true);//false：无说明，默认true
        //查询方式的优化,一般在服务端设置
        //request = request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //输出response结果
        printSearchHits(response);
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static SearchResponse operSearchAgg(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, AbstractAggregationBuilder[] aggBuilders) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, indexes, types, query, filter);
        for (int i = 0; i < aggBuilders.length; i++) {
            //TermsBuilder field = AggregationBuilders.terms("agg1").field("field");
            //DateHistogramBuilder field2 = AggregationBuilders.dateHistogram("agg2").field("birth").interval(DateHistogramInterval.YEAR);
            request = request.addAggregation(aggBuilders[i]);
        }
        SearchResponse response = request.get();
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    private static void operSearchAgg(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, AbstractAggregationBuilder[] aggBuilders, String[] aggNames) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, indexes, types, query, filter);
        for (int i = 0; i < aggBuilders.length; i++) {
//            TermsBuilder field = AggregationBuilders.terms("agg1").field("field");
//            DateHistogramBuilder field2 = AggregationBuilders.dateHistogram("agg2").field("birth").interval(DateHistogramInterval.YEAR);
            request = request.addAggregation(aggBuilders[i]);
        }
        SearchResponse response = request.get();
        // Get your facet results
        for (int i = 0; i < aggNames.length; i++) {
            System.out.println(i);
            Aggregation aggregation = response.getAggregations().get(aggNames[i]);
//            Map<String, Object> metaData = aggregation.getMetaData();
//            DateHistogram agg2 = sr.getAggregations().get("agg2");


//            //常用聚合函数
//            Min agg = response.getAggregations().get(aggNames[i]);
//            Max agg = response.getAggregations().get(aggNames[i]);
//            Sum agg = response.getAggregations().get(aggNames[i]);
//            Avg agg = response.getAggregations().get(aggNames[i]);
//            ValueCount agg = response.getAggregations().get(aggNames[i]);
//            //其他
//            Cardinality agg=(Cardinality)aggregation;     //基数
//            System.out.println(agg.getName()+":"+agg.getValue());
//            //常用聚合函数
//            Stats stats = response.getAggregations().get(aggNames[i]);
//            System.out.println(stats.getName()+
//                    ":\tmin:"+stats.getMinAsString()+
//                    "\tmax:"+stats.getMaxAsString()+
//                    "\tsum:"+stats.getSumAsString()+
//                    "\tavg:"+stats.getAvgAsString()+
//                    "\tcount:"+stats.getCountAsString());
//            //常用聚合函数+标准差，平方和，方差
//            ExtendedStats extendedStats = response.getAggregations().get(aggNames[i]);
//            System.out.println(extendedStats.getName()+
//                    ":\tmin:"+extendedStats.getMinAsString()+
//                    "\tmax:"+extendedStats.getMaxAsString()+
//                    "\tsum:"+extendedStats.getSumAsString()+
//                    "\tavg:"+extendedStats.getAvgAsString()+
//                    "\tcount:"+extendedStats.getCountAsString()+
//                    "\tstdDeviation:"+extendedStats.getStdDeviationAsString()+  //标准差
//                    "\tsumOfSquares:"+extendedStats.getSumOfSquaresAsString()+  //平方和
//                    "\tvariance:"+extendedStats.getVarianceAsString());         //方差
//            //抽取数据进行判断数据的一些东西。 参考：http://blog.csdn.net/asia_kobe/article/details/50170937
//            Percentiles pers=(Percentiles)aggregation;
//            for(Percentile per:pers){
//                //随机抽取百分多少的数据：XXX
//                System.out.println(per.getPercent()+":"+per.getValue());
//            }
//            //参考：https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html
//            PercentileRanks persRanks=(PercentileRanks)aggregation;
//            for(Percentile per:persRanks){
//                //随机抽取百分多少的数据：XXX
//                System.out.println(per.getPercent()+":"+per.getValue());
//            }
//            //获取边界【经纬度】参考：https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-geobounds-aggregation.html
//            GeoBounds geoBounds=(GeoBounds)aggregation;
//            System.out.println("bottomRight:"+geoBounds.bottomRight()+"\ttopLeft:"+geoBounds.topLeft());
//            //分组取几条数据（默认是3条）
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();                    // bucket key
//                long docCount = entry.getDocCount();            // Doc count
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                // We ask for top_hits for each bucket
//                TopHits topHits = entry.getAggregations().get("top");
//                for (SearchHit hit : topHits.getHits().getHits()) {
//                    System.out.println(" -> id:" + hit.getId() + ", _source" + hit.getSourceAsString());
//                }
//            }
//            //分组求元素个数总数,平均,和,最大值,最小值
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKey().toString();
//                long docCount = entry.getDocCount();
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                Avg chiHit = entry.getAggregations().get("avg");
//                Sum chiHit2 = entry.getAggregations().get("sum");
//                System.out.println("avg:" + chiHit.getValue() + "\tsum：" + chiHit2.getValue());
//            }


//            //********************************************************************
//            //父子表中：先根据父表的country分组，在设置子表，在根据子表的hobby进行分组。
//            //********************************************************************
//            Terms terms = (Terms) aggregation;
//            for (Terms.Bucket entry : terms.getBuckets()) {
//                String key = entry.getKeyAsString();
//                long docCount = entry.getDocCount();
//                System.out.println("key:" + key + ", doc_count:" + docCount);
//                Children children = entry.getAggregations().get("employee");
//                Terms chiTerms=children.getAggregations().get("hobby");
//                for (Terms.Bucket chiEntry : chiTerms.getBuckets()) {
//                    key=chiEntry.getKeyAsString();
//                    docCount = chiEntry.getDocCount();
//                    System.out.println("\t--> key:" + key + ", doc_count:" + docCount);
//                }
//            }


        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：根据pageSize分批次查询
     * 备注：
     * index:数据库
     * type：表
     * id:主键
     * time:获取之后存储的时间，单位毫秒
     * pageSize:每次滚动显示的条数
     */
    public static List<SearchResponse> operSearchScroll(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int time, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, indexes, types, query, filter);
        request = getRequestBySort(request, sorts);
        //获取之后存储的时间，单位毫秒
        TimeValue tValue;
        if (time > 0) {
            tValue = new TimeValue(time);
        } else {
            tValue = new TimeValue(3000);
        }
        request = request.setScroll(tValue);
        //每次滚动显示的条数
        if (pageSize > 0) {
            request = request.setSize(pageSize);
        } else {
            request = request.setSize(10);
        }
        SearchResponse scrollResp = request.get();
        ArrayList<SearchResponse> list = new ArrayList<>();
        for (int i = 1; ; i++) {
            list.add(scrollResp);
            System.out.println("第" + i + "次滚动：");
            printSearchHits(scrollResp);
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(tValue).get();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        return list;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static long operSearchCount(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, indexes, types, query, filter);
        request = request.setSize(0);//文档里面说的把size设置为0
        SearchResponse response = request.get();
        return response.getHits().getTotalHits();
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/5 19:26
     * 名称：关联查询【方法和DeleteByQuery插件类似】
     * 备注：
     */
    public static SearchResponse operSearchSiren(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, SortBuilder[] sorts, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        //DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
        SearchRequestBuilder request = new CoordinateSearchRequestBuilder(client);//根据上面方式,查找package包写的.
        //SearchRequestBuilder request = CoordinateSearchAction.INSTANCE.newRequestBuilder(client);//和上面是等价的
        request = request.setIndices(indexes);
        request = request.setTypes(types);
        if (query != null) {
            request = request.setQuery(query);
        }
        if (filter != null) {
            request = request.setPostFilter(filter);
        }
        //添加排序
        request = getRequestBySort(request, sorts);
        //分页
        request = getRequestByPage(request, pageIndex, pageSize);
        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        printSearchHits(response);//输出response结果
        return response;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：获取【还要优化】
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static List<SearchResponse> operSearchMulti(String[] indexes, String[] types, QueryBuilder[] querys, QueryBuilder filter, int pageIndex, int pageSize) {
        Client client = ESClientHelper.getInstance().getClient();
        MultiSearchRequestBuilder request = client.prepareMultiSearch();
        //request添加多个子request
        for (int i = 0; i < querys.length; i++) {
            //子的Request
            SearchRequestBuilder chiRequest = getRequest(client, indexes, types, querys[i], filter);
            request = request.add(chiRequest);
        }
        MultiSearchResponse multiResponse = request.get();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long sumHits = 0;
        float sumSeconds = 0f;
        ArrayList<SearchResponse> list = new ArrayList<>();
        for (MultiSearchResponse.Item item : multiResponse.getResponses()) {
            SearchResponse response = item.getResponse();
            list.add(response);
            sumHits += response.getHits().getTotalHits();
            sumSeconds += response.getTookInMillis() / 1000f;
        }
        System.out.println("各个条件总命中数：" + sumHits + "\t总时间（单位s）：" + sumSeconds);
        return list;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-15 下午5:34
     * 名称：查询【20个数据，查询（小于等于6）个才有效。30%以下才有效果？？？】
     * 备注：3个参数一个都不能少。
     * index:数据库
     * type：表
     * id:主键
     */
    public static SearchResponse operSearchTerminate(String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter, int maxNum) {
        Client client = ESClientHelper.getInstance().getClient();
        SearchRequestBuilder request = getRequest(client, indexes, types, query, filter);
        request = request.setSize(maxNum);
        //设置获取多少条数据就停止搜索
        request = request.setTerminateAfter(maxNum);

        SearchResponse response = request.get();//执行操作，并返回操作结果,底层调用//.execute().actionGet();
        //判断是否提前完成。
        if (response.isTerminatedEarly()) {
            // We finished early
            System.out.println("We finished early");
        }
        printSearchHits(response);
        return response;
    }


    /**
     * 作者: 王坤造
     * 日期: 16-11-23 下午4:59
     * 名称：将对象转化为json字符串
     * 备注：
     */
    public static String ObjectToJson(Object o) {
        //第0种方法
        return new Gson().toJson(o);
        //其他方法参考[版本]：
        // http://blog.csdn.net/napoay/article/details/51707023
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.3/java-docs-index.html

        //ObjectMapper mapper = new ObjectMapper();
        //byte[] json = mapper.writeValueAsBytes(obj);
    }

    private static Map<String, Object> printGetResponse(GetResponse response) {
        if (response.isExists()) {
            System.out.println("response.getId():" + response.getId() + ":" + response.isExists());
            System.out.println(response.getId() + ":" + response.getSourceAsString() + "\tversion:" + response.getVersion());
            return response.getSourceAsMap();
        } else {
            return null;
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-17 下午7:33
     * 名称：打印输出SearchResponse所有元素
     * 备注：
     */
    private static List<Map<String, Object>> printGetResponses(MultiGetResponse responses) {
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        GetResponse response;
        Map<String, Object> map;
        for (MultiGetItemResponse itemResponse : responses) {
            response = itemResponse.getResponse();
            map = printGetResponse(response);
            if (map != null) {
                list.add(map);
            }
        }
        return list;
    }

    private static SearchRequestBuilder getRequest(Client client, String[] indexes, String[] types, QueryBuilder query, QueryBuilder filter) {
        SearchRequestBuilder request = client.prepareSearch(indexes);
        if (types != null && types.length > 0) {
            request = request.setTypes(types);
        }
        if (query != null) {
            request = request.setQuery(query);
        }
        if (filter != null) {
            request = request.setPostFilter(filter);
        }
        return request;
    }

    private static SearchRequestBuilder getRequestByFields(SearchRequestBuilder request, String[] fields) {
        //添加要显示的字段
        if (fields != null && fields.length > 0) {
            request = request.addFields(fields);
        }
        return request;
    }

    private static SearchRequestBuilder getRequestBySort(SearchRequestBuilder request, SortBuilder[] sorts) {
        //添加排序
        if (sorts != null && sorts.length > 0) {
            //SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME).order(SortOrder.ASC);
            for (SortBuilder sort : sorts) {
                request = request.addSort(sort);
            }
        }
        return request;
    }

    private static SearchRequestBuilder getRequestByPage(SearchRequestBuilder request, int pageIndex, int pageSize) {
        //分页
        if (pageIndex > 0 || pageSize > 0) {
            pageIndex = pageIndex > 0 ? pageIndex : 1;
            pageSize = pageSize > 0 ? pageSize : 10;
            request = request.setFrom((pageIndex - 1) * pageSize).setSize(pageSize);
        } else {
            //不分页【默认显示10条】
            //request=request.setSize(10);
        }
        return request;
    }

    /**
     * 作者: 王坤造
     * 日期: 16-11-17 下午7:33
     * 名称：打印输出SearchResponse所有元素
     * 备注：
     */
    private static void printSearchHits(SearchResponse response) {
        SearchHits hits = response.getHits();
        System.out.println("命中数：" + hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + ":" + hit.getSourceAsString() + "\tscore:" + hit.getScore());
        }
    }

}


/**
 * 作者: 王坤造
 * 日期: 2016/12/5 11:21
 * 名称：获取es客户端
 * 备注：至少需要2个参数:集群名称,es集群中一台节点的ip端口号
 */
class ESClientHelper {
    //集群名称
    public static String clusterName = "testes";
    //存储es的ip,host
    private HashMap<String, Integer> ips = new HashMap<String, Integer>() {{
        put("192.168.2.124", 9300);
    }};
    private ConcurrentHashMap<String, Client> clientMap = new ConcurrentHashMap<String, Client>();

    private ESClientHelper() {
        if (clientMap.size() < 1) {
            init();
        }
    }

    private void init() {
        //配置参数参考：http://blog.csdn.net/ljc2008110/article/details/48630609
        //Settings settings = ImmutableSettings.settingsBuilder()
        Settings setting = Settings.settingsBuilder()
                //服务器的集群名称,如果集群名称不匹配则报如下错误。
                //node null not part of the cluster Cluster [elasti], ignoring...
                //Exception in thread "main" NoNodeAvailableException[None of the configured nodes are available: [{#transport#-1}{192.168.1.128}{192.168.1.128:9300}]]
                .put("cluster.name", clusterName)
                //使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中，
                //这样做的好处是你不用手动设置集群里所有集群的ip到连接客户端，
                //它会自动帮你添加，并且自动发现新加入集群的机器。
                .put("client.transport.sniff", true)
                .build();
        InetSocketTransportAddress[] transportAddress = getAllAddress(ips);
        addClient(setting, getAllAddress(ips));
    }

    private void addClient(Settings setting, InetSocketTransportAddress[] transportAddress) {
        Client client = TransportClient.builder().settings(setting)
                .addPlugin(DeleteByQueryPlugin.class)//添加delete-by-query插件
                .addPlugin(SirenJoinPlugin.class)//添加siren插件
                .build()
                //上面有添加client.transport.sniff配置，所以这里添加一台机器就OK【如果没有修改端口号，则API客户端的端口号为9300，集群测试的时候用9200】
                .addTransportAddresses(transportAddress);
        clientMap.put(clusterName, client);
    }

    private InetSocketTransportAddress[] getAllAddress(HashMap<String, Integer> ips) {
        InetSocketTransportAddress[] addressList = new InetSocketTransportAddress[ips.size()];
        try {
            int i = 0;
            for (String ip : ips.keySet()) {
                addressList[i] = new InetSocketTransportAddress(InetAddress.getByName(ip), ips.get(ip));
                i++;
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return addressList;
    }


    public Client getClient() {
        return clientMap.get(clusterName);
    }

    public static final ESClientHelper getInstance() {
        return ClientHolder.INSTANCE;
    }

    private static class ClientHolder {
        private static final ESClientHelper INSTANCE = new ESClientHelper();
    }
}
