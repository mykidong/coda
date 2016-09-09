package io.shunters.coda.store;

import io.shunters.coda.message.MessageList;
import io.shunters.coda.offset.QueueShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2016-09-09.
 */
public class StoreManager implements StoreHandler {

    private static Logger log = LoggerFactory.getLogger(StoreManager.class);

    private static StoreHandler storeHandler;

    private static final Object lock = new Object();

    private ConcurrentMap<QueueShard, List<Segment>> segmentMap;

    public static StoreHandler getInstance()
    {
        if(storeHandler == null)
        {
            synchronized (lock)
            {
                if(storeHandler == null)
                {
                    storeHandler = new StoreManager();
                }
            }
        }
        return storeHandler;
    }

    private StoreManager()
    {
        segmentMap = new ConcurrentHashMap<>();

        // TODO: load segments and offset index files.
    }

    private int getSegmentIndex(QueueShard queueShard, long offset)
    {
        List<Segment> segments = this.segmentMap.get(queueShard);

        int first = 0;
        int last = segments.size() -1;
        while(first <= last)
        {
            int middle = (first + last) / 2;
            long retOffset = segments.get(middle).getBaseOffset();
            if(retOffset < offset)
            {
                first = middle + 1;
            }
            else if(retOffset > offset)
            {
                last = middle -1;
            }
            else
            {
                return middle;
            }
        }

        return last;
    }


    @Override
    public void add(QueueShard queueShard, long firstOffset, MessageList messageList) {
        Segment segment = null;
        if(segmentMap.containsKey(queueShard))
        {
            segment = segmentMap.get(queueShard).get(this.getSegmentIndex(queueShard, firstOffset));
            segment.add(firstOffset, messageList);
        }
        else
        {
            // TODO: base directory for segment and index to be configurable.
            long baseOffset = 1;
            OffsetIndex offsetIndex = new OffsetIndex(new File("/tmp/" + baseOffset + ".index"), baseOffset);
            segment = new Segment(new File("/tmp/" + baseOffset + ".seg"), baseOffset, offsetIndex);
            segment.add(firstOffset, messageList);

            List<Segment> segments = new ArrayList<>();
            segments.add(segment);

            segmentMap.put(queueShard, segments);
        }
    }

    @Override
    public MessageList getMessageList(QueueShard queueShard, long offset, int maxByteSize) {
        if(segmentMap.containsKey(queueShard))
        {
            Segment segment = segmentMap.get(queueShard).get(this.getSegmentIndex(queueShard, offset));
            return segment.getMessageList(offset, maxByteSize);
        }
        else {
            log.warn("queue shard [{}] does not exist.", queueShard.toString());
            return null;
        }
    }
}
