package io.shunters.coda.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by mykidong on 2016-08-29.
 */
public class NioSelector {

    private static Logger log = LoggerFactory.getLogger(NioSelector.class);

    private Selector selector;

    private Map<String, SocketChannel> channelMap;


    public static NioSelector open()
    {
        return new NioSelector();
    }

    private NioSelector()
    {
        channelMap = new HashMap<>();

        try {
            this.selector = Selector.open();
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }


    public void register(String channelId, SocketChannel socketChannel, int interestOps)
    {
        this.channelMap.put(channelId, socketChannel);

        try {
            socketChannel.register(this.selector, interestOps);
        }catch (ClosedChannelException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void register(SocketChannel socketChannel, int interestOps)
    {

        try {
            socketChannel.register(this.selector, interestOps);
        }catch (ClosedChannelException e)
        {
            throw new RuntimeException(e);
        }
    }


    public void attach(String channelId, int interestOps, Object attachment)
    {
        SocketChannel socketChannel = this.channelMap.get(channelId);

        if(socketChannel != null) {
            this.attach(socketChannel, interestOps, attachment);
        }
        else
        {
            log.warn("socket channel for channelId [{}] is null.", channelId);
        }
    }


    public void attach(SocketChannel socketChannel, int interestOps, Object attachment)
    {
        try
        {
            socketChannel.register(this.selector, interestOps, attachment);
        }catch (ClosedChannelException e)
        {
            throw new RuntimeException(e);
        }
    }

    public SelectionKey interestOps(SocketChannel socketChannel, int interestOps)
    {
        return socketChannel.keyFor(this.selector).interestOps(interestOps);
    }


    public void interestOpsWithAttachment(String channelId, int interestOps, Object attachment)
    {

        SelectionKey selectionKey = interestOps(channelId, interestOps);
        selectionKey.attach(attachment);
    }

    public SelectionKey interestOps(String channelId, int interestOps)
    {

        return this.channelMap.get(channelId).keyFor(this.selector).interestOps(interestOps);
    }

    public Selector wakeup()
    {
        return this.selector.wakeup();
    }

    public int select()
    {
        try {
            return this.selector.select();
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Set<SelectionKey> selectedKeys()
    {
        return this.selector.selectedKeys();
    }

    public static String makeChannelId(SocketChannel socketChannel)
    {
        String localHost = socketChannel.socket().getLocalAddress().getHostAddress();
        int localPort = socketChannel.socket().getLocalPort();
        String remoteHost = socketChannel.socket().getInetAddress().getHostAddress();
        int remotePort = socketChannel.socket().getPort();

        StringBuffer sb = new StringBuffer();
        sb.append(localHost).append(":").append(localPort).append("-").append(remoteHost).append(":").append(remotePort);

        return sb.toString();
    }
}
