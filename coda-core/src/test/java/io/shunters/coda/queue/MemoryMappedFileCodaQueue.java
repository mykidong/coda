package io.shunters.coda.queue;

import io.shunters.coda.store.OffsetIndex;
import io.shunters.coda.store.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mykidong on 2016-10-21.
 */
public class MemoryMappedFileCodaQueue implements CodaQueue {

    private static Logger log = LoggerFactory.getLogger(Segment.class);

    private FileChannel fileChannel;
    private AtomicLong size = new AtomicLong(0);
    private AtomicLong currentReadPosition = new AtomicLong(0);

    private final ReentrantLock lock = new ReentrantLock();


    public MemoryMappedFileCodaQueue(String path)
    {
        File file = new File(path);
        try {
            if(!file.exists()) {
                file.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();

            size.set(raf.length());
            log.info("initial size [{}]", size);
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
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


    @Override
    public void add(Object object) {
        lock.lock();
        try
        {
            int currentPosition = (int) size.get();
            fileChannel.position(currentPosition);

            // TODO: Just String Type is valid for a while.
            if(object instanceof String) {
                String value = (String) object;

                byte[] bytes = value.getBytes();

                int length = bytes.length;
                int total = 4 + length;

                ByteBuffer buffer = ByteBuffer.allocate(total);
                buffer.putInt(length);
                buffer.put(bytes);
                buffer.rewind();

                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }

                size.addAndGet(total);
            }
            else
            {
                throw new RuntimeException("Value type must be String!");
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public Object get() {
        String value = null;

        if(size.get() == 0)
        {
            return null;
        }

        lock.lock();
        try
        {
            value = getWithFileChannel();
        }
        finally {
            lock.unlock();
        }

        return value;
    }


    private String getWithFileChannel()
    {
        try {
            fileChannel.position((int) currentReadPosition.get());

            ByteBuffer bufferLength = ByteBuffer.allocate(4);
            fileChannel.read(bufferLength);
            bufferLength.rewind();

            int length = bufferLength.getInt();
            if (length <= 0) {
                return null;
            }

            ByteBuffer buffer = ByteBuffer.allocate(length);
            fileChannel.position((int) currentReadPosition.get() + 4);
            fileChannel.read(buffer);

            buffer.rewind();
            byte[] bytes = new byte[length];
            buffer.get(bytes);

            int total = 4 + length;

            String v = new String(bytes);

            currentReadPosition.addAndGet(total);

            return v;
        }catch (IOException e)
        {
            e.printStackTrace();

            return null;
        }
    }
}
