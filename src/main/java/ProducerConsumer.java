import SolrModel.Company;
import SolrModel.DiscoverDrugs;
import com.google.gson.Gson;

import java.util.List;
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
                        ESUtilsNew.operIndex(index, type, c1.getId(), 0, null, c1);
                        break;
                    case "discoverdrugs":
                        DiscoverDrugs c2 = (DiscoverDrugs) product.model;
                        ESUtilsNew.operIndex(index, type, c2.getId(), 0, null, c2);
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

    BlockingQueue<Product> queues = new LinkedBlockingQueue<Product>(110);

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
