package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;
import io.shunters.coda.offset.QueueShard;
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

    public SortMessageListProcessor()
    {
        this.messageListMap = new ConcurrentHashMap<>();

        // TODO: set period.
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


                // TODO: send to StoreProcessor.
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
