package io.shunters.coda.util;

import io.shunters.coda.ProducerTestSkip;
import io.shunters.coda.store.OffsetIndex;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by mykidong on 2016-09-07.
 */
public class OffsetIndexTestSkip {

    @Before
    public void init() throws Exception {
        java.net.URL url = new ProducerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);
    }

    @Test
    public void add() throws Exception
    {
        File file = new File("/tmp/coda-data/test.index");
        if(file.exists())
        {
            file.delete();
        }

        file.createNewFile();

        OffsetIndex offsetIndex = new OffsetIndex(file, 500);

        offsetIndex.add(550, 100);
        offsetIndex.add(560, 200);
        offsetIndex.add(570, 300);
        offsetIndex.add(580, 400);

        OffsetIndex.OffsetPosition offsetPositionForFirstOffset = offsetIndex.getFirstOffsetPosition(560);
        Assert.assertTrue(offsetPositionForFirstOffset.getOffset() == 560);
        Assert.assertTrue(offsetPositionForFirstOffset.getPosition() == 200);

        offsetIndex.add(565, 250);
        OffsetIndex.OffsetPosition offsetPositionForArbitaryOffset = offsetIndex.getFirstOffsetPosition(568);
        Assert.assertTrue(offsetPositionForArbitaryOffset.getOffset() == 565);
        Assert.assertTrue(offsetPositionForArbitaryOffset.getPosition() == 250);

        offsetIndex.printEntries();
    }

    @Test
    public void raf() throws Exception
    {
        File file = new File("/tmp/coda-data/test.index");
        if(file.exists())
        {
            file.delete();
        }

        file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        MappedByteBuffer mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 500);

        mmap.putInt(5);
        mmap.putInt(10);
        mmap.putInt(15);
        mmap.putInt(20);


        byte[] slice = new byte[8];
        ByteBuffer buffer = mmap.duplicate();
        buffer.position(8);
        buffer.get(slice);

        mmap.position(8);
        mmap.putInt(13);
        mmap.put(slice);

        for(int i = 0; i < 5; i++)
        {
            int value = mmap.getInt(i * 4);
            System.out.println(value);
        }

    }
}
