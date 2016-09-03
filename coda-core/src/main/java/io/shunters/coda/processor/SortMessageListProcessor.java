package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.offset.QueueShard;
import io.shunters.coda.util.DisruptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2016-09-03.
 */
public class SortMessageListProcessor extends AbstractQueueThread<SortMessageListEvent> implements EventHandler<SortMessageListEvent> {

    private static Logger log = LoggerFactory.getLogger(SortMessageListProcessor.class);

    private ConcurrentMap<QueueShard, List<AddMessageListEvent.QueueShardMessageList>> messageListMap;

    private final Object lock = new Object();

    private Disruptor<StoreEvent> storeDisruptor;
    private StoreEventTranslator storeEventTranslator;

    public SortMessageListProcessor()
    {
        this.messageListMap = new ConcurrentHashMap<>();

        StoreProcessor storeProcessor = new StoreProcessor();
        storeProcessor.start();
        this.storeDisruptor = DisruptorBuilder.singleton("Store", StoreEvent.FACTORY, 1024, storeProcessor);
        this.storeEventTranslator = new StoreEventTranslator();

        // TODO: set period by configuration.
        CustomTimer customTimer = new CustomTimer(this, 500);
        customTimer.runTimer();
    }



    @Override
    public void onEvent(SortMessageListEvent memStoreEvent, long l, boolean b) throws Exception {
        this.put(memStoreEvent);
    }

    @Override
    public void process(SortMessageListEvent event) {

        synchronized (lock) {
            List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists = event.getQueueShardMessageLists();
            for (AddMessageListEvent.QueueShardMessageList queueShardMessageList : queueShardMessageLists) {
                QueueShard queueShard = queueShardMessageList.getQueueShard();

                if (this.messageListMap.containsKey(queueShard)) {
                    this.messageListMap.get(queueShard).add(queueShardMessageList);
                } else {
                    List<AddMessageListEvent.QueueShardMessageList> messageLists = new ArrayList<>();
                    messageLists.add(queueShardMessageList);

                    this.messageListMap.put(queueShard, messageLists);
                }
            }
        }
    }

    public void flush()
    {
        synchronized (lock)
        {
            for(QueueShard queueShard : messageListMap.keySet())
            {
                List<AddMessageListEvent.QueueShardMessageList> messageList = this.messageListMap.remove(queueShard);

                // sort by firstOffset.
                Collections.sort(messageList, new Comparator<AddMessageListEvent.QueueShardMessageList>() {
                    @Override
                    public int compare(AddMessageListEvent.QueueShardMessageList o1, AddMessageListEvent.QueueShardMessageList o2) {
                        return Long.compare(o1.getFirstOffset(), o2.getFirstOffset());
                    }
                });

                // send to StoreProcessor.
                this.storeEventTranslator.setQueueShard(queueShard);
                this.storeEventTranslator.setQueueShardMessageLists(messageList);
                this.storeDisruptor.publishEvent(this.storeEventTranslator);
            }
        }
    }


    private static class CustomTimer {
        private CustomTimerTask timerTask;
        private int delay = 1000;
        private int period;
        private Timer timer;

        private SortMessageListProcessor sortMessageListProcessor;

        public CustomTimer(SortMessageListProcessor sortMessageListProcessor, int interval) {
            this.period = interval;

            this.sortMessageListProcessor = sortMessageListProcessor;

            this.timerTask = new CustomTimerTask(this.sortMessageListProcessor);
            this.timer = new Timer();
        }

        public void runTimer() {
            this.timer.scheduleAtFixedRate(this.timerTask, delay, period);
        }

        public void reset() {
            this.timerTask.cancel();
            this.timer.purge();
            this.timerTask = new CustomTimerTask(this.sortMessageListProcessor);
        }
    }

    public static class CustomTimerTask extends TimerTask {
        private SortMessageListProcessor sortMessageListProcessor;

        public CustomTimerTask(
                SortMessageListProcessor sortMessageListProcessor) {
            this.sortMessageListProcessor = sortMessageListProcessor;
        }

        @Override
        public void run() {
            try {
                this.sortMessageListProcessor.flush();

            } catch (Exception e) {
                log.error("Exception", e);
            }
        }
    }
}
