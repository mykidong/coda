package io.shunters.coda.store;

import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mykidong on 2016-09-07.
 */
public class Segment {
    private static Logger log = LoggerFactory.getLogger(Segment.class);

    private long baseOffset;
    private RandomAccessFile raf;
    private MappedByteBuffer mmap;
    private OffsetIndex offsetIndex;
    private int size = 0;
    private long entryCount = 0;

    private final ReentrantLock lock = new ReentrantLock();

    public Segment(File file, long baseOffset, OffsetIndex offsetIndex)
    {
        this.baseOffset = baseOffset;
        this.offsetIndex = offsetIndex;
        try {
            boolean isNew = file.createNewFile();
            raf = new RandomAccessFile(file, "rw");

            // TODO: file length to be configurable.
            long length = (raf.length() == 0) ? 1024 * 1024 * 1024 : raf.length();
            raf.setLength(length);
            mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, length);
            if(isNew)
            {
                mmap.position(0);
            }

            initSize();
            log.info("initial size [{}]", size);
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }finally {
            try {
                raf.close();
            }catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    private void initSize()
    {
        ByteBuffer buffer = mmap.duplicate();
        buffer.rewind();
        while(buffer.hasRemaining())
        {
            MessageList messageList = MessageList.fromByteBuffer(buffer);
            int length = messageList.length();
            // if length equals to just bytes size of MessageOffset list count.
            if(length == 4)
            {
                break;
            }

            size += messageList.length();
            entryCount++;
        }
    }

    public void add(long firstOffset, MessageList messageList)
    {
        lock.lock();
        try
        {
            int currentPosition = mmap.position();

            // add offset position to offset index file.
            offsetIndex.add(firstOffset, currentPosition);

            // add MessageList to segment file.
            messageList.writeToBuffer(mmap);
            size += messageList.length();
            entryCount++;
        }finally {
            lock.unlock();
        }
    }

    public MessageList getMessageList(long offset, int maxByteSize)
    {
        ByteBuffer buffer = mmap.duplicate();

        int lengthSum = 0;
        long currentOffset = offset;
        List<MessageOffset> messageOffsetList = new ArrayList<>();
        while(lengthSum < maxByteSize) {
            OffsetIndex.OffsetPosition offsetPosition = offsetIndex.getFirstOffsetPosition(currentOffset);
            int position = offsetPosition.getPosition();

            buffer.position(position);
            MessageList messageList = MessageList.fromByteBuffer(buffer);

            for (MessageOffset messageOffset : messageList.getMessageOffsets()) {
                long tempOffset = messageOffset.getOffset();
                if (offset <= tempOffset) {
                    currentOffset = tempOffset;
                    messageOffsetList.add(messageOffset);
                    lengthSum += messageOffset.length();
                    if (maxByteSize < lengthSum) {
                        break;
                    }
                }
            }
            currentOffset++;
        }

        return new MessageList(messageOffsetList);
    }


    public void printEntries()
    {
        ByteBuffer buffer = mmap.duplicate();
        buffer.rewind();
        while(buffer.hasRemaining())
        {
            MessageList messageList = MessageList.fromByteBuffer(buffer);
            int length = messageList.length();
            // if length equals to just bytes size of MessageOffset list count.
            if(length == 4)
            {
                break;
            }

            log.info("message list [{}]", messageList.toString());
        }
    }
}