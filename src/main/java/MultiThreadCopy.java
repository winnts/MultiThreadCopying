import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by adyachenko on 27.10.15.
 */
public class MultiThreadCopy{

    final static AtomicInteger indexCount = new AtomicInteger(2);
    final static LongAdder numberInIndex = new LongAdder();
    final static LongAdder flushCount = new LongAdder();
    static Client client = ConnectAsNode.connectASNode().client();

    static final BulkProcessor bulkProcessor = BulkProcessor.builder(
            client,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
//                            System.out.println("################ " + Thread.currentThread().getName() + " #############################");
//                            System.out.println("Bulk count = " + numberInIndex);
//                           System.out.println("Flush count = " + flushCount);
//                            CountResponse countResponse = getCount();
//                            System.out.println("Total records = " + countResponse.getCount());
//                            System.out.println("#############################################");
//                            if (countResponse.getCount() >= 100 * 1000 * 1000) {
//                                indexCount.incrementAndGet();
//                            }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    for (BulkItemResponse bulkItemResponse : response) {
                        numberInIndex.increment();
                        if(bulkItemResponse.getFailureMessage()!=null) {
                            System.err.println(Thread.currentThread().getName() + " " + bulkItemResponse.getId() + " " + bulkItemResponse.getFailureMessage());
                        }
                    }
                    checkIndexSize();
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                }
            })
            .setBulkActions(500)
                    //.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
            .setBulkSize(new ByteSizeValue(-1))
            .setFlushInterval(TimeValue.timeValueSeconds(60))
            .setConcurrentRequests(1)
            .build();

    public void copyESIndices(String fromDate, String toDate) throws IOException {
        FilteredQueryBuilder fqb = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.andFilter(FilterBuilders.termFilter("status", "true"), FilterBuilders.rangeFilter("added").from(fromDate).to(toDate)));
            SearchResponse scrollResp = client.prepareSearch("domains")
                    .setSearchType(SearchType.SCAN)
                    .setScroll(new TimeValue(120000))
                    .setQuery(fqb)
                    .setSize(100).execute().actionGet();
            System.out.println(scrollResp.getHits().getTotalHits());
            while (true) {
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    bulkProcessor.add(new IndexRequest(getIndexName(), "doc", hit.getId()).source(hit.getSource()));
                    //System.out.println(Thread.currentThread().getName()+": " + hit.getSource().get("domain").toString());
                }
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(1200000)).execute().actionGet();
                if (scrollResp.getHits().getHits().length == 0) {
                    break;
                }
            }
    }

    private static String getIndexName() {
        return "domains00" + indexCount.get();
    }

    private static void checkIndexSize() {
     //   System.out.println("numberInIndex = " + String.format("%,03d", numberInIndex.longValue()));
        if(numberInIndex.longValue() > 70 * 1000 * 1000) {
            System.out.println("index increment...");
            indexCount.incrementAndGet();
            System.out.println("indexCount = " + indexCount.get());
            numberInIndex.reset();
            numberInIndex.add(getCount());
        }
    }

    public static long getCount() {
        FilteredQueryBuilder fqb = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), null);
        return client.prepareCount(getIndexName())
                                        .setQuery(fqb)
                                        .execute()
                                        .actionGet().getCount();
    }

    public static synchronized void write2File(String arg) {
        try {
            FileWriter fileWriter = new FileWriter("completed_dates.txt", true);
            fileWriter.write(arg + "\n");
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-5);
        }

    }

    public static void main(String[] args) throws InterruptedException {
        numberInIndex.add(getCount());
        System.out.println("Starting with " + String.format("%,03d", numberInIndex.longValue()) + " docs ");
        String[] dates = new String[]{
                "2015/06/01","2015/06/02","2015/06/03","2015/06/04","2015/06/05",
                "2015/06/06","2015/06/07","2015/06/08","2015/06/09","2015/06/10",
                "2015/06/11","2015/06/12","2015/06/13","2015/06/14","2015/06/15",
                "2015/06/16","2015/06/17","2015/06/18","2015/06/19","2015/06/20",
                "2015/06/21","2015/06/22","2015/06/23","2015/06/24","2015/06/25",
                "2015/06/26","2015/06/27","2015/06/28","2015/06/29","2015/06/30"
               //,"2015/06/31"


        };
        String[] hours = new String[]{"00","01","02","03","04","05","06","07","08","09","10","11",
                                      "12","13","14","15","16","17","18","19","20","21","22","23"};
        List<Thread> threads = new ArrayList<>();
        for (String date : dates) {
            for (String hour : hours) {
                Thread copy1 = new Thread() {
                    public void run() {
                        setName(date + " " + hour);
                        MultiThreadCopy copy1 = new MultiThreadCopy();
                        try {
                            String fromDate = date + " " + hour + ":00:00.000";
                            String toDate = date + " " + hour + ":59:59.999";
                            copy1.copyESIndices(fromDate, toDate);
                            write2File(fromDate + " - " + toDate);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        System.out.println(Thread.currentThread().getName() + " finished");
                    }
                };
                copy1.setDaemon(true);
                copy1.start();
                threads.add(copy1);
            }
        }
        for (Thread thread : threads) {
            thread.join();
        }
        bulkProcessor.flush();
        bulkProcessor.close();
        client.close();
       // ConnectAsNode.connectASNode().close();
        System.out.println("DONE");
    }
}
