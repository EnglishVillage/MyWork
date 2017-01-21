package MyWork;

import com.google.gson.Gson;
import elasticsearch.api.ESUtils;

import javax.annotation.Resource;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 名称：王坤造
 * 时间：2016/12/8.
 * 名称：
 * 备注：
 */
public class SolrDataToESWriter {
//    @Resource
//    private static ESUtils ESUtils;
    public static void main(String[] args) throws Exception {
        final long start = System.currentTimeMillis();
        //String[] indexes = {"company","discoverdrugs","discoverdrugscountry","discoverdrugsnamedic","discoverdrugssalse","discoverlateststage","discovertargetlateststage","indication","target"};
        String[] indexes = {"company"};
        ExecutorService service = Executors.newFixedThreadPool(20);
        for (String name : indexes) {
            String index = name;
            String type = name;
            String path = "D:/MyDocument/Cube/SolrToES/solr_data/" + name + "/";
            File[] files = new File(path).listFiles();

            //singleThread(index, type, GSON,files);


            multithread1(service, index, type, files);
        }

        try {
            service.shutdown();
            if (!service.awaitTermination(1, TimeUnit.HOURS)) {
                service.shutdownNow();
            }
        } catch (Exception e) {
            service.shutdownNow();
            throw e;
        }
        final long end = System.currentTimeMillis();
        System.out.println((end - start) + "ms");//29s

//        生产
//        里面    527ms   530ms   554ms   543ms   526ms   529ms
//        537ms 519ms   533ms   603ms   646ms   680ms   638ms
//        外面    520ms   513ms   512ms   520ms   506ms   492ms
//        520ms 513ms   512ms   520ms   506ms   492ms   10thread
//        557ms 516ms   569ms   534ms   546ms   571ms   555ms
//        508ms 519ms   527ms   490ms   520ms   483ms   15thread
//        504ms 565ms   553ms   574ms   553ms   592ms   545ms

//        510ms 554ms   523ms   531ms   547ms   604ms   20thread
//        591ms 865ms   613ms   548ms   547ms   529ms


        //1612
//        3947ms 10thread
//        15thread
    }

    private static void multithread1(ExecutorService service, String index, String type, File[] files) throws Exception {
        index = index + "2";
        Storage s = new Storage();
        multiProduce(service, index, type, files, s);
        multiConsume(service, s);
    }

    private static void multiConsume(ExecutorService service, Storage s) {
        ConsumerWithJsonId con = new ConsumerWithJsonId(s, 7000, "con");
        service.submit(con);
        ConsumerWithJsonId con2 = new ConsumerWithJsonId(s, 7000, "con2");
        service.submit(con2);
        ConsumerWithJsonId con3 = new ConsumerWithJsonId(s, 7000, "con3");
        service.submit(con3);
        ConsumerWithJsonId con4 = new ConsumerWithJsonId(s, 7000, "con4");
        service.submit(con4);
        ConsumerWithJsonId con5 = new ConsumerWithJsonId(s, 7000, "con5");
        service.submit(con5);
        ConsumerWithJsonId con6 = new ConsumerWithJsonId(s, 7000, "con6");
        service.submit(con6);
        ConsumerWithJsonId con7 = new ConsumerWithJsonId(s, 7000, "con7");
        service.submit(con7);
        ConsumerWithJsonId con8 = new ConsumerWithJsonId(s, 7000, "con8");
        service.submit(con8);
        ConsumerWithJsonId con9 = new ConsumerWithJsonId(s, 7000, "con9");
        service.submit(con9);
        ConsumerWithJsonId con10 = new ConsumerWithJsonId(s, 7000, "con10");
        service.submit(con10);
//        ConsumerWithJsonId con11 = new ConsumerWithJsonId( s, 7000, "con11");
//        service.submit(con11);
//        ConsumerWithJsonId con12 = new ConsumerWithJsonId( s, 7000, "con12");
//        service.submit(con12);
//        ConsumerWithJsonId con13 = new ConsumerWithJsonId( s, 7000, "con13");
//        service.submit(con13);
//        ConsumerWithJsonId con14 = new ConsumerWithJsonId( s, 7000, "con14");
//        service.submit(con14);
//        ConsumerWithJsonId con15 = new ConsumerWithJsonId( s, 7000, "con15");
//        service.submit(con15);
//        ConsumerWithJsonId con16 = new ConsumerWithJsonId( s, 7000, "con16");
//        service.submit(con16);
//        ConsumerWithJsonId con17 = new ConsumerWithJsonId( s, 7000, "con17");
//        service.submit(con17);
//        ConsumerWithJsonId con18 = new ConsumerWithJsonId( s, 7000, "con18");
//        service.submit(con18);
//        ConsumerWithJsonId con19 = new ConsumerWithJsonId( s, 7000, "con19");
//        service.submit(con19);
//        ConsumerWithJsonId con20 = new ConsumerWithJsonId( s, 7000, "con20");
//        service.submit(con20);
    }

    private static void multiProduce(ExecutorService service, String index, String type, File[] files, Storage s) throws Exception {
//        ExecutorService service = Executors.newFixedThreadPool(10);
        //几个线程读
        String idName = "id";
        LinkedBlockingQueue<String> list = new LinkedBlockingQueue<>();
        for (File file : files) {
            list.addAll(getQueue(file));
        }
        ProducerWithJsonId pro = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro);
        ProducerWithJsonId pro2 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro2);
        ProducerWithJsonId pro3 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro3);
        ProducerWithJsonId pro4 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro4);
        ProducerWithJsonId pro5 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro5);
        ProducerWithJsonId pro6 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro6);
        ProducerWithJsonId pro7 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro7);
        ProducerWithJsonId pro8 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro8);
        ProducerWithJsonId pro9 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro9);
        ProducerWithJsonId pro10 = new ProducerWithJsonId(index, type, idName, list, s);
        service.submit(pro10);
//        ProducerWithJsonId pro11 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro11);
//        ProducerWithJsonId pro12 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro12);
//        ProducerWithJsonId pro13 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro13);
//        ProducerWithJsonId pro14 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro14);
//        ProducerWithJsonId pro15 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro15);
//        ProducerWithJsonId pro16 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro16);
//        ProducerWithJsonId pro17 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro17);
//        ProducerWithJsonId pro18 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro18);
//        ProducerWithJsonId pro19 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro19);
//        ProducerWithJsonId pro20 = new ProducerWithJsonId(index, type, idName, list, s);
//        service.submit(pro20);
    }

    private static void multithreadOld(ExecutorService service, String index, String type, File[] files, Storage s) throws Exception {
        index = index + "2";
        String idName = "id";
        for (File file : files) {
            BlockingQueue<String> list = getQueue(file);

            ProducerWithJsonId pro = new ProducerWithJsonId(index, type, idName, list, s);
            ProducerWithJsonId pro2 = new ProducerWithJsonId(index, type, idName, list, s);
            service.submit(pro);
            service.submit(pro2);
            ConsumerWithJsonId con = new ConsumerWithJsonId(s, 7000,file.getName() + "AAA");
            service.submit(con);

//            ConsumerWithJsonId con2 = new ConsumerWithJsonId(index, type, s, file.getName() + "hahaha");
//            service.submit(con2);
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
