package MyWork;

import SolrModel.*;
import com.google.gson.Gson;
import elasticsearch.api.ESUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 名称：王坤造
 * 时间：2016/12/9.
 * 名称：
 * 备注：
 */
public class ProducerConsumer {

}

/**
 * 消费者
 *
 * @author WKZ
 * @version 1.0 2013-7-24 下午04:53:30
 */
class Consumer<T> implements Runnable {
    private String index;
    private String type;
    private Storage s = null;

    public Consumer(String index, String type, Storage s) {
        this.index = index;
        this.type = type;
        this.s = s;
    }

    public void run() {
        long timeBegin = System.currentTimeMillis();
        while (true) {
            try {
                Product product = s.pop();
                System.out.println("消费:\t"+s.queues.size());
                switch (type) {
                    case "company":
                        Company c1 = (Company) product.model;
                        ESUtils.operIndex(index, type, c1.getId(), 0, null, c1);
                        break;
                    case "discoverdrugs":
                        DiscoverDrugs c2 = (DiscoverDrugs) product.model;
                        ESUtils.operIndex(index, type, c2.getId(), 0, null, c2);
                        break;
                    case "discoverdrugscountry":
                        DiscoverDrugsCountry c3 = (DiscoverDrugsCountry) product.model;
                        ESUtils.operIndex(index, type, c3.getId(), 0, null, c3);
                        break;
                    case "discoverdrugsnamedic":
                        DiscoverDrugsNameDic c4 = (DiscoverDrugsNameDic) product.model;
                        ESUtils.operIndex(index, type, c4.getId(), 0, null, c4);
                        break;
                    case "discoverdrugssalse":
                        DiscoverDrugsSalse c5 = (DiscoverDrugsSalse) product.model;
                        ESUtils.operIndex(index, type, c5.getId(), 0, null, c5);
                        break;
                    case "discoverlateststage":
                        DiscoverLatestStage c6 = (DiscoverLatestStage) product.model;
                        ESUtils.operIndex(index, type, c6.getId(), 0, null, c6);
                        break;
                    case "discovertargetlateststage":
                        DiscoverTargetLatestStage c7 = (DiscoverTargetLatestStage) product.model;
                        ESUtils.operIndex(index, type, c7.getId(), 0, null, c7);
                        break;
                    case "indication":
                        Indication c8 = (Indication) product.model;
                        ESUtils.operIndex(index, type, c8.getId(), 0, null, c8);
                        break;
                    case "target":
                        Target c9 = (Target) product.model;
                        ESUtils.operIndex(index, type, c9.getId(), 0, null, c9);
                        break;
                }
                if(s.isOK && s.queues.isEmpty()){
                    long timeEnd = System.currentTimeMillis();
                    System.out.println("time:\t"+(timeEnd-timeBegin+"ms"));
                    break;
                }
            } catch (InterruptedException e) {
                System.err.println("push err:");
                e.printStackTrace();
            }
        }
    }
}

/**
 * 生产者
 *
 * @author WKZ
 * @version 1.0 2013-7-24 下午04:53:44
 */
class Producer implements Runnable {
    private String index;
    private String type;
    private BlockingQueue<String> list;
    private Storage s = null;
    Gson gson = new Gson();

    public Producer(String index, String type, BlockingQueue<String> list, Storage s) {
        this.index = index;
        this.type = type;
        this.list = list;
        this.s = s;
    }

    public void run() {
//            //将list转化为线程安全的list
//            List<String> newList = Collections.synchronizedList(list);
        while(!list.isEmpty()){
            String json = null;
            try {
                json = list.take();
            } catch (InterruptedException e) {
                System.err.println("take err:");
                e.printStackTrace();
            }
            Object obj = null;
            switch (type) {
                case "company":
                    obj = gson.fromJson(json, Company.class);
                    break;
                case "discoverdrugs":
                    obj = gson.fromJson(json, DiscoverDrugs.class);
                    break;
                case "discoverdrugscountry":
                    obj = gson.fromJson(json, DiscoverDrugsCountry.class);
                    break;
                case "discoverdrugsnamedic":
                    obj = gson.fromJson(json, DiscoverDrugsNameDic.class);
                    break;
                case "discoverdrugssalse":
                    obj = gson.fromJson(json, DiscoverDrugsSalse.class);
                    break;
                case "discoverlateststage":
                    obj = gson.fromJson(json, DiscoverLatestStage.class);
                    break;
                case "discovertargetlateststage":
                    obj = gson.fromJson(json, DiscoverTargetLatestStage.class);
                    break;
                case "indication":
                    obj = gson.fromJson(json, Indication.class);
                    break;
                case "target":
                    obj = gson.fromJson(json, Target.class);
                    break;
            }
            try {
                s.push(new Product(index, type, obj));
                System.out.println("生产:\t"+s.queues.size());
            } catch (InterruptedException e) {
                System.err.println("push err:");
                e.printStackTrace();
            }
            //System.out.println(s.queues.size());
        }
        s.isOK = true;
    }
}


class Storage {
    public boolean isOK = false;

    //不指定大小
    BlockingQueue<Product> queues = new LinkedBlockingQueue<Product>();

    /**
     * 生产
     *
     * @param p 产品
     * @throws InterruptedException
     */
    public void push(Product p) throws InterruptedException {
        queues.put(p);
    }

    /**
     * 消费
     *
     * @return 产品
     * @throws InterruptedException
     */
    public Product pop() throws InterruptedException {
        return queues.take();
    }
}

class Product {
    public String index;
    public String type;
    public Object model;

    public Product(String index, String type, Object model) {
        this.index = index;
        this.type = type;
        this.model = model;
    }
}
