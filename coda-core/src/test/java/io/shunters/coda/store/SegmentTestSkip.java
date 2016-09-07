package io.shunters.coda.store;

import io.shunters.coda.ProducerTestSkip;
import io.shunters.coda.ServerTestSkip;
import io.shunters.coda.message.Message;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageOffset;
import io.shunters.coda.message.MessageTest;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2016-09-07.
 */
public class SegmentTestSkip {

    private static Logger log = LoggerFactory.getLogger(SegmentTestSkip.class);

    @Before
    public void init() throws Exception {
        java.net.URL url = new ProducerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);
    }

    @Test
    public void add() throws Exception
    {
        OffsetIndex offsetIndex = buildOffsetIndex("/tmp/coda-data/500.index", 500);
        Segment segment = buildSegment("/tmp/coda-data/500.seg", 500, offsetIndex);

        for(int i = 0; i < 5; i++) {
            addMessageList(segment, i);
        }
        segment.printEntries();

        MessageList messageList = segment.getMessageList(525, 5000);
        log.info(messageList.toString());
    }

    private void addMessageList(Segment segment, int n) {
        List<MessageOffset> messageOffsetList = new ArrayList<>();
        long delta = 10;
        long firstOffset = 500 + (delta * n);
        for(int i = 0; i < delta; i++)
        {
            long offset = firstOffset + i;
            messageOffsetList.add(new MessageOffset(offset, MessageTest.buildInstance()));
        }

        MessageList messageList = new MessageList(messageOffsetList);

        segment.add(firstOffset, messageList);
    }

    private OffsetIndex buildOffsetIndex(String filePath, long baseOffset) throws Exception
    {
        File file = new File(filePath);
        if(file.exists())
        {
            file.delete();
        }

        file.createNewFile();

        return new OffsetIndex(file, baseOffset);
    }

    private Segment buildSegment(String filePath, long baseOffset, OffsetIndex offsetIndex) throws Exception
    {
        File file = new File(filePath);
        if(file.exists())
        {
            file.delete();
        }

        file.createNewFile();

        return new Segment(file, baseOffset, offsetIndex);
    }
}
