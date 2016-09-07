package io.shunters.coda.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mykidong on 2016-09-07.
 */
public class OffsetIndex {
    private static Logger log = LoggerFactory.getLogger(OffsetIndex.class);

    private final ReentrantLock lock = new ReentrantLock();

    private long baseOffset;
    private RandomAccessFile raf;
    private MappedByteBuffer mmap;
    private int entryCount = 0;
    private long lastOffset = 0;

    public OffsetIndex(File file, long baseOffset)
    {
        this.baseOffset = baseOffset;
        try {
            boolean isNew = file.createNewFile();
            raf = new RandomAccessFile(file, "rw");

            // TODO: file length to be configurable.
            long length = 1024 * 1024 * 10; // 10MB.
            raf.setLength(length);
            mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, length);
            if(isNew)
            {
                mmap.position(0);
            }

            initEntryCount();
            log.info("initial entry count [{}]", entryCount);

            readLastOffset();
            log.info("last offset [{}]", lastOffset);
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

    private void initEntryCount()
    {
        ByteBuffer buffer = mmap.duplicate();
        buffer.rewind();
        while (buffer.hasRemaining())
        {
            int value = buffer.getInt(entryCount * 8);
            if(value <= 0)
            {
                break;
            }

            entryCount++;
        }
    }

    private void readLastOffset()
    {
        if(entryCount > 0)
        {
            this.lastOffset = baseOffset + this.getDeltaOffset(mmap, entryCount -1);
        }
    }

    public void printEntries()
    {
        ByteBuffer buffer = mmap.duplicate();
        for(int i = 0; i < entryCount; i++)
        {
            long firstOffset = baseOffset + getDeltaOffset(buffer, i);
            int position = getPosition(buffer, i);
            log.info("(firstOffset, position) = ({}, {})", firstOffset, position);
        }
    }

    public void add(long firstOffset, int position)
    {
        lock.lock();
        try {
            if(lastOffset < firstOffset) {
                mmap.putInt((int) (firstOffset - baseOffset));
                mmap.putInt(position);
                entryCount++;
                lastOffset = firstOffset;
            }
            else
            {
                // search for the entry index whose offset value is greater than the target offset(firstOffset) and difference between them is smallest.
                ByteBuffer buffer = mmap.duplicate();
                int first = 0;
                int last = entryCount -1;
                while(first <= last)
                {
                    int middle = (first + last) / 2;
                    long retOffset = baseOffset + getDeltaOffset(buffer, middle);
                    if(retOffset < firstOffset)
                    {
                        first = middle + 1;
                    }
                    else if(retOffset > firstOffset)
                    {
                        last = middle -1;
                    }
                    else
                    {
                        throw new RuntimeException("Offset [" + firstOffset + "] Already exists.");
                    }
                }

                int entryIndex = first;

                // slice the buffer from the chosen entry index.
                byte[] lastBuffer = new byte[(entryCount * 8) - (entryIndex * 8)];
                buffer.position(entryIndex * 8);
                buffer.get(lastBuffer);

                // put new offset entry.
                mmap.position(entryIndex * 8);
                mmap.putInt((int) (firstOffset - baseOffset));
                mmap.putInt(position);

                // after that, append the sliced buffer.
                mmap.put(lastBuffer);
                entryCount++;
            }
        }finally {
            lock.unlock();
        }
    }


    public void flush()
    {
        lock.lock();
        try {
            mmap.force();
        }finally {
            lock.unlock();
        }
    }

    private int getEntryIndex(ByteBuffer buffer, long offset)
    {
        int first = 0;
        int last = entryCount -1;
        while(first <= last)
        {
            int middle = (first + last) / 2;
            long retOffset = baseOffset + getDeltaOffset(buffer, middle);
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

    public OffsetPosition getFirstOffsetPosition(long offset)
    {
        ByteBuffer buffer = mmap.duplicate();
        int entryIndex = this.getEntryIndex(buffer, offset);

        long firstOffset = baseOffset + getDeltaOffset(buffer, entryIndex);
        int position = getPosition(buffer, entryIndex);

        return new OffsetPosition(firstOffset, position);
    }

    private int getDeltaOffset(ByteBuffer buffer, int n)
    {
        return buffer.getInt(n * 8);
    }

    private int getPosition(ByteBuffer buffer, int n)
    {
        return buffer.getInt(n * 8 + 4);
    }


    public static class OffsetPosition
    {
        private long offset;
        private int position;

        public OffsetPosition(long offset, int position)
        {
            this.offset = offset;
            this.position = position;
        }

        public long getOffset() {
            return offset;
        }

        public int getPosition() {
            return position;
        }
    }
}
