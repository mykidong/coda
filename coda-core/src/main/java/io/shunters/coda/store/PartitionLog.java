package io.shunters.coda.store;

import io.shunters.coda.deser.AvroDeSer;
import io.shunters.coda.util.SingletonUtils;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mykidong on 2017-08-30.
 */
public class PartitionLog {

    private static Logger log = LoggerFactory.getLogger(PartitionLog.class);

    /**
     * avro de-/serialization.
     */
    private static AvroDeSer avroDeSer = SingletonUtils.getAvroDeSerSingleton();

    private long baseOffset;
    private FileChannel fileChannel;
    private OffsetIndex offsetIndex;
    private long size = 0;

    private final ReentrantLock lock = new ReentrantLock();

    public PartitionLog(File file, long baseOffset, OffsetIndex offsetIndex) {
        this.baseOffset = baseOffset;
        this.offsetIndex = offsetIndex;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();

            size = raf.length();
            log.info("initial size [{}]", size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    private ByteBuffer getMMap(int position, long length) {
        try {
            return fileChannel.map(FileChannel.MapMode.READ_WRITE, position, length).duplicate();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void add(long firstOffset, GenericRecord records) {
        lock.lock();
        try {
            int currentPosition = (int) size;

            byte[] avroBytes = avroDeSer.serialize(records);

            int dataSize = avroBytes.length;

            // add offset position to offset index file.
            offsetIndex.add(firstOffset, currentPosition, dataSize);

            fileChannel.position(currentPosition);

            ByteBuffer buffer = ByteBuffer.wrap(avroBytes);
            buffer.rewind();

            // add avro records to segment file.
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }

            size += avroBytes.length;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}

