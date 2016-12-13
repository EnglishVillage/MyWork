import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 名称：王坤造
 * 时间：2016/12/7.
 * 名称：
 * 备注：
 */
public class SolrReader {

    public static void main(String[] args) throws Exception {
        getHttpSolrServer();
    }

    public static void getHttpSolrServer() throws Exception {
        String[] indexes = {"company","discoverDrugs","discoverDrugsCountry","discoverDrugsNameDic","discoverDrugsSalse","discoverLatestStage","discoverTargetLatestStage","indication","target"};
        for (String index : indexes) {
            HttpSolrServer solr = new HttpSolrServer("http://192.168.1.136:8983/solr/" + index);
            solr.setMaxRetries(2);
            solr.setConnectionTimeout(5000);
            solr.setMaxTotalConnections(100);
            solr.setAllowCompression(true);

            SolrQuery query = new SolrQuery();
            query.setQuery("*:*");
            query.setRows(0);
            QueryResponse response = solr.query(query);
            SolrDocumentList results = response.getResults();
            long totalCount = results.getNumFound();//当前index总条数
            //System.out.println(index + ":\t" + totalCount);

            int pageSize = 10;
            List<String> list = new ArrayList<>();
            Gson gson = new Gson();
            long totalTimeBegin = System.currentTimeMillis();
            long perTimeBegin = totalTimeBegin;
            long perTimeEnd;
            int j;
            int k = 0;
            long curCount=0;//总条数
            String json;
            for (int i = 0; i < totalCount; i += 100) {
                list.clear();
                for (j = 0; j < pageSize; j++) {
                    query.setStart(i + j * pageSize);
                    query.setRows(pageSize);
                    response = solr.query(query);
                    results = response.getResults();
                    //String jsons = gson.toJson(results);//得到集合json字符串,不能插入到es中
                    //if (jsons.equals("[]")) {//空的集合就退出
                    //    break;
                    //} else {
                    //    list.add(jsons);
                    //}
                    if (results.size() > 0) {
                        for (k = 0; k < results.size(); k++) {
                            json = gson.toJson(results.get(k));//得到单个对象json字符串
                            list.add(json);
                        }
                    } else {//空的集合就退出
                        break;
                    }
                }
                curCount = i + (j - 1) * 10 + k;
                readFileByLine(list, "D:/MyDocument/Cube/SolrToES/solr_data/" + index + "/" + index.substring(0, 1) + curCount + ".json");
                perTimeEnd = System.currentTimeMillis();
                System.out.println("\t" + index + "_No" + curCount + ":\t" + (perTimeEnd - perTimeBegin) + "ms");
                perTimeBegin = perTimeEnd;
            }
            long totalTimeEnd = System.currentTimeMillis();
            System.out.println(index + "_Total" + curCount + ":\t" + (totalTimeEnd - totalTimeBegin) + "ms");
        }
    }

    /**
     * 以行为单位读写文件内容
     *
     * @param filePath
     */
    public static void readFileByLine(List<String> list, String filePath) throws Exception {
        BufferedWriter bufWriter = null;
        try {
            bufWriter = new BufferedWriter(new FileWriter(filePath));
            for (String line : list) {
                bufWriter.write(line + System.lineSeparator());
            }
        } catch (FileNotFoundException e) {
            boolean isCreate = new File(filePath.substring(0, filePath.lastIndexOf("/"))).mkdir();
            if (isCreate) {
                bufWriter = new BufferedWriter(new FileWriter(filePath));
                for (String line : list) {
                    bufWriter.write(line + System.lineSeparator());
                }
            } else {
                throw e;
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (bufWriter != null) {
                try {
                    bufWriter.close();
                } catch (Exception e) {
                    e.getStackTrace();
                }
            }
        }
    }
}
