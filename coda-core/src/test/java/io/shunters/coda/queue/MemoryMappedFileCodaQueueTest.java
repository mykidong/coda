package io.shunters.coda.queue;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.metrics.MetricsReporter;
import io.shunters.coda.metrics.SystemOutMetricsReporter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by mykidong on 2016-10-21.
 */
public class MemoryMappedFileCodaQueueTest {

    private int size;
    private int thread;
    private List<String> list;

    @Before
    public void init() {
        size = Integer.parseInt(System.getProperty("size", "40000"));
        thread = Integer.parseInt(System.getProperty("thread", "16"));

        list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add("hello, this is coda queue entry....................................[" + i + "]");
        }
    }

    @Test
    public void add() {
        String path = "/tmp/coda-queue";
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }

        CodaQueue queue = new MemoryMappedFileCodaQueue(path);

        queue.add("hello-coda-queue1");
        queue.add("hello-coda-queue2");
        queue.add("hello-coda-queue3");

        for (int i = 0; i < 3; i++) {
            System.out.println("[" + (String) queue.get() + "]");
        }
    }

    @Test
    public void testMultiThreadedProducer() {
        String path = "/tmp/coda-queue";
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }

        CodaQueue queue = new MemoryMappedFileCodaQueue(path);

        // producer.
        runBenchmark(list.size(), thread, new QueueProducer(list, queue));
    }

    @Test
    public void testMultiThreadedConsumer() {
        String path = "/tmp/coda-queue";
        CodaQueue queue = new MemoryMappedFileCodaQueue(path);

        // consumer.
        runBenchmark(list.size(), thread, new QueueConsumer(queue));
    }

    private void runBenchmark(int listSize, int threadSize, Callable task) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadSize);
        List<Future<Integer>> resultList = new ArrayList<>();

        long start = System.nanoTime();

        for (int i = 0; i < threadSize * 2; i++) {
            Future<Integer> result = executor.submit(task);
            resultList.add(result);
        }

        for (Future<Integer> future : resultList) {
            try {
                System.out.println("Future result - " + " - " + future.get() + "; And Task done is " + future.isDone());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        long elapsedTime = System.nanoTime() - start;
        System.out.println("elapsedTime: [" + elapsedTime / (1000 * 1000) + "]ms");

        int totalSize = listSize * threadSize * 2;
        System.out.println("totalSize: [" + totalSize + "]");

        System.out.println("tps: [" + ((double) totalSize / (double) elapsedTime * (1000 * 1000 * 1000)) + "]");

        //shut down the executor service now
        executor.shutdown();
    }

    private static class QueueProducer implements Callable<Integer> {
        private List<String> list;
        private CodaQueue queue;

        public QueueProducer(List<String> list, CodaQueue queue) {
            this.list = list;
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {

            for (String v : list) {
                this.queue.add(Thread.currentThread().getId() + ": " + v);
            }

            return list.size();
        }
    }

    private static class QueueConsumer implements Callable<Integer> {
        private CodaQueue queue;

        public QueueConsumer(CodaQueue queue) {
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            int count = 0;
            while (true) {
                Object ret = this.queue.get();
                if (ret != null && ((String) ret).length() > 0) {
                    //System.out.println("v: [" + (String) ret + "]");

                    count++;
                } else {
                    break;
                }
            }

            return count;
        }
    }


    // -----------------------------------------------------------------------------
    //
    // the benchmark of producing and consuming for memory mapped file based queue.
    //
    // Benchmark result: produce/consume tps is about 40 K / sec.
    //
    // -----------------------------------------------------------------------------

    @Test
    public void testMultiThreadedProducerConsumer() throws Exception {
        int thread = Integer.parseInt(System.getProperty("thread", "8"));

        String path = "/tmp/coda-queue";
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }

        CodaQueue queue = new MemoryMappedFileCodaQueue(path);

        MetricRegistry metricRegistry = MetricRegistryFactory.getInstance();
        MetricsReporter metricsReporter = new SystemOutMetricsReporter(metricRegistry);
        metricsReporter.start();

        // producer.
        ExecutorService producers = Executors.newFixedThreadPool(thread);
        for (int i = 0; i < thread * 2; i++) {
            producers.execute(new QueueProducerTask(queue, metricRegistry));
        }

        // consumer.
        ExecutorService consumers = Executors.newFixedThreadPool(thread);
        metricsReporter.start();
        for (int i = 0; i < thread * 2; i++) {
            consumers.execute(new QueueConsumerTask(queue, metricRegistry));
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    private static class QueueProducerTask implements Runnable {
        private CodaQueue queue;
        private MetricRegistry metricRegistry;

        public QueueProducerTask(CodaQueue queue, MetricRegistry metricRegistry) {
            this.queue = queue;
            this.metricRegistry = metricRegistry;
        }

        @Override
        public void run() {
            long count = 0;

            while (true) {
                String entry = "hello, this is coda queue entry....................................[" + Thread.currentThread() + ", " + count + "]";
                this.queue.add(entry);

                this.metricRegistry.meter("coda-queue-producer").mark();

                count++;
            }
        }
    }

    private static class QueueConsumerTask implements Runnable {
        private CodaQueue queue;
        private MetricRegistry metricRegistry;

        public QueueConsumerTask(CodaQueue queue, MetricRegistry metricRegistry) {
            this.queue = queue;
            this.metricRegistry = metricRegistry;
        }

        @Override
        public void run() {
            while (true) {
                Object ret = this.queue.get();
                if (ret != null && ((String) ret).length() > 0) {
                    this.metricRegistry.meter("coda-queue-consumer").mark();
                }
            }
        }
    }

    // -----------------------------------------------------------------------------
    //
    // the benchmark of producing and consuming for disruptor queue.
    //
    // Benchmark result: produce/consume tps is about 2 M / sec.
    //                   while mmaped queue is about 40 K / sec.
    //
    // WINNER IS DISRUPTOR!!!!
    //
    // -----------------------------------------------------------------------------

    @Test
    public void testMultiThreadedProducerConsumerWithDisruptor() throws Exception {
        int thread = Integer.parseInt(System.getProperty("thread", "8"));

        MetricRegistry metricRegistry = MetricRegistryFactory.getInstance();
        MetricsReporter metricsReporter = new SystemOutMetricsReporter(metricRegistry);
        metricsReporter.start();

        DisruptorFactory.ConsumeHandler handler = new DisruptorFactory.ConsumeHandler(metricRegistry);
        Disruptor<DisruptorFactory.QueueMessage> disruptor = DisruptorFactory.getDisruptor(handler);

        // producer.
        ExecutorService producers = Executors.newFixedThreadPool(thread);
        for (int i = 0; i < thread * 2; i++) {
            producers.execute(new QueueProducerDisruptorTask(disruptor, metricRegistry));
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    private static class QueueProducerDisruptorTask implements Runnable {
        private Disruptor<DisruptorFactory.QueueMessage> disruptor;
        private MetricRegistry metricRegistry;

        private DisruptorFactory.QueueMessageTranslator queueMessageTranslator;

        public QueueProducerDisruptorTask(Disruptor<DisruptorFactory.QueueMessage> disruptor, MetricRegistry metricRegistry) {
            this.disruptor = disruptor;
            this.metricRegistry = metricRegistry;

            this.queueMessageTranslator = new DisruptorFactory.QueueMessageTranslator();
        }

        @Override
        public void run() {
            long count = 0;

            while (true) {
                String entry = "hello, this is disruptor entry....................................[" + Thread.currentThread() + ", " + count + "]";
                this.queueMessageTranslator.setValue(entry);
                this.disruptor.publishEvent(queueMessageTranslator);

                this.metricRegistry.meter("disruptor-producer").mark();

                count++;
            }
        }
    }

    private static class DisruptorFactory {
        public static Disruptor<QueueMessage> getDisruptor(EventHandler... handlers) {
            Disruptor disruptor = new Disruptor(QueueMessage.EVENT_FACTORY,
                    1024,
                    Executors.newCachedThreadPool(),
                    ProducerType.SINGLE, // Single producer
                    new BlockingWaitStrategy());

            disruptor.handleEventsWith(handlers);
            disruptor.start();

            return disruptor;
        }

        private static class QueueMessage {
            private String value;

            public String getValue() {
                return value;
            }

            public void setValue(String value) {
                this.value = value;
            }

            public final static EventFactory<QueueMessage> EVENT_FACTORY = new EventFactory<QueueMessage>() {
                public QueueMessage newInstance() {
                    return new QueueMessage();
                }
            };
        }

        private static class QueueMessageTranslator implements EventTranslator<QueueMessage> {
            private String value;

            public void setValue(String value) {
                this.value = value;
            }

            @Override
            public void translateTo(QueueMessage queueMessage, long l) {
                queueMessage.setValue(value);
            }
        }

        private static class ConsumeHandler implements EventHandler<QueueMessage> {
            private MetricRegistry metricRegistry;

            public ConsumeHandler(MetricRegistry metricRegistry) {
                this.metricRegistry = metricRegistry;
            }

            @Override
            public void onEvent(QueueMessage queueMessage, long l, boolean b) throws Exception {
                if (queueMessage.getValue() != null) {
                    this.metricRegistry.meter("disruptor-consumer").mark();
                }
            }
        }
    }
}
