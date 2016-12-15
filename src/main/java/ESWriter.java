import SolrModel.*;
import com.google.gson.Gson;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 名称：王坤造
 * 时间：2016/12/8.
 * 名称：
 * 备注：
 */
public class ESWriter {
    public static void main(String[] args) throws Exception {
//String[] indexes = {"company","discoverdrugs","discoverdrugscountry","discoverdrugsnamedic","discoverdrugssalse","discoverlateststage","discovertargetlateststage","indication","target"};
        String[] indexes = { "target"};
        ExecutorService service = Executors.newCachedThreadPool();
        Gson gson = new Gson();
        for (String name : indexes) {
            String index = name;
            String type = name;
            String path = "D:/MyDocument/Cube/SolrToES/solr_data/" + name + "/";
            File[] files = new File(path).listFiles();

            //singleThread(index, type, gson,files);

            Storage s = new Storage();
            multithread(service, index, type, files, s);
        }
    }

    private static void multithread(ExecutorService service, String index, String type, File[] files, Storage s) throws Exception {
        //index=index+"2";
        for (File file : files) {
            BlockingQueue<String> list = getQueue(file);
            Producer pro = new Producer(index, type, list, s);
            Producer pro2 = new Producer(index, type, list, s);
            Consumer con = new Consumer(index, type, s);
            //Consumer con2 = new Consumer(index,type,s);
            service.submit(pro);
            service.submit(pro2);
            service.submit(con);
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/9 18:21
     * 名称：单线程写json到es(速度太慢)
     * 备注：
     */
    private static void singleThread(String index, String type, Gson gson, File[] files) throws Exception {
        for (File file : files) {
            List<String> list = getList(file);
            for (String json : list) {
                switch (index) {
                    case "company":
                        Company c1 = gson.fromJson(json, Company.class);
                        ESUtilsNew.operIndex(index, type, c1.getId(), 0, null, c1);
                        break;
                    case "discoverdrugs":
                        DiscoverDrugs c2 = gson.fromJson(json, DiscoverDrugs.class);
                        ESUtilsNew.operIndex(index, type, c2.getId(), 0, null, c2);
                        break;
                    case "discoverdrugscountry":
                        DiscoverDrugsCountry c3 = gson.fromJson(json, DiscoverDrugsCountry.class);
                        ESUtilsNew.operIndex(index, type, c3.getId(), 0, null, c3);
                        break;
                    case "discoverdrugsnamedic":
                        DiscoverDrugsNameDic c4 = gson.fromJson(json, DiscoverDrugsNameDic.class);
                        ESUtilsNew.operIndex(index, type, c4.getId(), 0, null, c4);
                        break;
                    case "discoverdrugssalse":
                        DiscoverDrugsSalse c5 = gson.fromJson(json, DiscoverDrugsSalse.class);
                        ESUtilsNew.operIndex(index, type, c5.getId(), 0, null, c5);
                        break;
                    case "discoverlateststage":
                        DiscoverLatestStage c6 = gson.fromJson(json, DiscoverLatestStage.class);
                        ESUtilsNew.operIndex(index, type, c6.getId(), 0, null, c6);
                        break;
                    case "discovertargetlateststage":
                        DiscoverTargetLatestStage c7 = gson.fromJson(json, DiscoverTargetLatestStage.class);
                        ESUtilsNew.operIndex(index, type, c7.getId(), 0, null, c7);
                        break;
                    case "indication":
                        Indication c8 = gson.fromJson(json, Indication.class);
                        ESUtilsNew.operIndex(index, type, c8.getId(), 0, null, c8);
                        break;
                    case "target":
                        Target c9 = gson.fromJson(json, Target.class);
                        ESUtilsNew.operIndex(index, type, c9.getId(), 0, null, c9);
                        break;
                }
            }
        }
    }

    public static List<String> getList(File file) throws Exception {
        List<String> list = new ArrayList<>();
        BufferedReader bufr = null;
        String temp;
        try {
            bufr = new BufferedReader(new FileReader(file));
            while ((temp = bufr.readLine()) != null) {
                list.add(temp);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            bufr.close();
        }
        return list;
    }

    public static LinkedBlockingQueue<String> getQueue(File file) throws Exception {
        LinkedBlockingQueue<String> queues = new LinkedBlockingQueue<String>();
        BufferedReader bufr = null;
        String temp;
        try {
            bufr = new BufferedReader(new FileReader(file));
            while ((temp = bufr.readLine()) != null) {
                queues.add(temp);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            bufr.close();
        }
        return queues;
    }
}
