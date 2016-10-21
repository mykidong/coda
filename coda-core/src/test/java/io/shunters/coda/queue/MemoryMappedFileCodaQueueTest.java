package io.shunters.coda.queue;

import com.codahale.metrics.MetricRegistry;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.metrics.MetricsReporter;
import io.shunters.coda.metrics.SystemOutMetricsReporter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by mykidong on 2016-10-21.
 */
public class MemoryMappedFileCodaQueueTest {

    @Test
    public void add()
    {
        String path = "/tmp/coda-queue";
        File f = new File(path);
        if(f.exists())
        {
            f.delete();
        }

        CodaQueue queue = new MemoryMappedFileCodaQueue(path);

        queue.add("hello-coda-queue1");
        queue.add("hello-coda-queue2");
        queue.add("hello-coda-queue3");

        for(int i = 0; i < 3; i++) {
            System.out.println("[" + (String) queue.get() + "]");
        }
    }

    @Test
    public void testMultiThreading()
    {
        int size = Integer.parseInt(System.getProperty("size", "500000"));
        int thread = Integer.parseInt(System.getProperty("thread", "5"));

        int MAX_THREAD = thread;
        int MAX = size;

        List<String> list = new ArrayList<>();

        for(int i = 0; i < MAX; i++)
        {
            list.add("hello, this is coda queue entry....................................[" + i + "]");
        }

        String path = "/tmp/coda-queue";
        File f = new File(path);
        if(f.exists())
        {
            f.delete();
        }

        CodaQueue queue = new MemoryMappedFileCodaQueue(path);

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREAD);

        List<Future<Integer>> resultList = new ArrayList<>();

        long start = System.nanoTime();

        // producer.
        for (int i = 0; i < MAX_THREAD * 2; i++) {
            Future<Integer> result = executor.submit(new Task(list, queue));
            resultList.add(result);
        }

        // consumer.
        ExecutorService consumers = Executors.newFixedThreadPool(MAX_THREAD);
        MetricRegistry metricRegistry = MetricRegistryFactory.getInstance();
        MetricsReporter metricsReporter = new SystemOutMetricsReporter(metricRegistry);
        metricsReporter.start();
        for (int i = 0; i < MAX_THREAD * 2; i++) {
            consumers.execute(new QueueConsumer(queue, metricRegistry));
        }

        for (Future<Integer> future : resultList) {
            try {
                System.out.println("Future result is - " + " - " + future.get() + "; And Task done is " + future.isDone());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        long elapsedTime = System.nanoTime() - start;
        System.out.println("elapsedTime: [" + elapsedTime / (1000 * 1000) + "]ms");

        int totalSize = list.size() * MAX_THREAD * 2;
        System.out.println("totalSize: [" + totalSize + "]");

        System.out.println("tps: [" + ((double) totalSize / (double)elapsedTime * (1000 * 1000 * 1000)) + "]");

        //shut down the executor service now
        executor.shutdown();
    }

    private static class Task implements Callable<Integer>
    {
        private List<String> list;
        private CodaQueue queue;

        public Task(List<String> list, CodaQueue queue)
        {
            this.list = list;
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {

            for(String v : list)
            {
                this.queue.add(v);
            }

            return list.size();
        }
    }

    private static class QueueConsumer implements Runnable {
        private CodaQueue queue;
        private MetricRegistry metricRegistry;

        public QueueConsumer(CodaQueue queue, MetricRegistry metricRegistry) {
            this.queue = queue;
            this.metricRegistry = metricRegistry;


        }

        @Override
        public void run() {
            while (true)
            {
                Object ret = this.queue.get();
                if(ret != null && ((String)ret).length() > 0)
                {
                    this.metricRegistry.meter("coda-queue-consumer").mark();
                }
            }
        }
    }
}
