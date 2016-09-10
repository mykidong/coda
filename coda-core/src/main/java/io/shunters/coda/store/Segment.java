package io.shunters.coda.store;

import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO: Rolling segment file to offset.
 */
public class Segment {
    private static Logger log = LoggerFactory.getLogger(Segment.class);

    private long baseOffset;
    private FileChannel fileChannel;
    private OffsetIndex offsetIndex;
    private long size = 0;

    private final ReentrantLock lock = new ReentrantLock();

    public Segment(File file, long baseOffset, OffsetIndex offsetIndex)
    {
        this.baseOffset = baseOffset;
        this.offsetIndex = offsetIndex;
        try {
            if(!file.exists()) {
                file.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();

            size = raf.length();
            log.info("initial size [{}]", size);
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    private ByteBuffer getMMap(int position, long length)
    {
        try {
            return fileChannel.map(FileChannel.MapMode.READ_WRITE, position, length).duplicate();
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void add(long firstOffset, MessageList messageList)
    {
        lock.lock();
        try
        {
            int currentPosition = (int) size;

            // add offset position to offset index file.
            offsetIndex.add(firstOffset, currentPosition);

            fileChannel.position(currentPosition);

            ByteBuffer buffer = ByteBuffer.allocate(messageList.length());
            messageList.writeToBuffer(buffer);
            buffer.rewind();

            // add MessageList to segment file.
            while(buffer.hasRemaining())
            {
                fileChannel.write(buffer);
            }
            size += messageList.length();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        finally {
            lock.unlock();
        }
    }

    public GetMessageList getMessageList(long offset, int maxByteSize)
    {
        if(size == 0)
        {
            return null;
        }

        int lengthSum = 0;
        long currentOffset = offset;
        List<MessageOffset> messageOffsetList = new ArrayList<>();
        while(lengthSum < maxByteSize) {
            OffsetIndex.OffsetPosition offsetPosition = offsetIndex.getFirstOffsetPosition(currentOffset);
            if(offsetPosition == null)
            {
                break;
            }

            int position = offsetPosition.getPosition();

            // TODO: it may cause to be memory-exhausting???
            ByteBuffer buffer = getMMap(position, size);
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

        return new GetMessageList(currentOffset, (maxByteSize - lengthSum), new MessageList(messageOffsetList));
    }


    public void printEntries()
    {
        if(size == 0)
        {
            log.info("no entries to print");
        }

        ByteBuffer buffer = getMMap(0, size);
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

    public static class GetMessageList
    {
        private long currentOffset;
        private int maxByteSizeRemained;
        private MessageList messageList;

        public GetMessageList(long currentOffset, int maxByteSizeRemained, MessageList messageList)
        {
            this.currentOffset = currentOffset;
            this.maxByteSizeRemained = maxByteSizeRemained;
            this.messageList = messageList;
        }

        public long getCurrentOffset() {
            return currentOffset;
        }

        public int getMaxByteSizeRemained() {
            return maxByteSizeRemained;
        }

        public MessageList getMessageList() {
            return messageList;
        }
    }
}