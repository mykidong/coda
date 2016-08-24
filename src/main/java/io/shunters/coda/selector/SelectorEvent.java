package io.shunters.coda.selector;

import com.lmax.disruptor.EventFactory;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Created by mykidong on 2016-08-23.
 */
public class SelectorEvent {

    private SelectionKey key;

    private SocketChannel socketChannel;

    public SelectionKey getKey() {
        return key;
    }

    public void setKey(SelectionKey key) {
        this.key = key;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public static final EventFactory<SelectorEvent> FACTORY = new EventFactory<SelectorEvent>() {
        @Override
        public SelectorEvent newInstance() {
            return new SelectorEvent();
        }
    };
}
