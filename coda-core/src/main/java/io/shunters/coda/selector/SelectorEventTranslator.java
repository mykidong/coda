package io.shunters.coda.selector;

import com.lmax.disruptor.EventTranslator;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Created by mykidong on 2016-08-23.
 */
public class SelectorEventTranslator implements EventTranslator<SelectorEvent> {

    private SelectionKey key;

    private SocketChannel socketChannel;

    public void setKey(SelectionKey key) {
        this.key = key;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    @Override
    public void translateTo(SelectorEvent selectorEvent, long l) {
        selectorEvent.setKey(this.key);
        selectorEvent.setSocketChannel(this.socketChannel);
    }
}
