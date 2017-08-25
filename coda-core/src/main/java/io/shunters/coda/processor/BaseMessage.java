package io.shunters.coda.processor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import org.apache.avro.generic.GenericRecord;

/**
 * Created by mykidong on 2017-08-25.
 */
public class BaseMessage {

    public static class BaseHeader
    {
        private String channelId;
        private NioSelector nioSelector;

        private short apiKey;
        private short apiVersion;
        private byte messageFormat;

        public String getChannelId() {
            return channelId;
        }

        public void setChannelId(String channelId) {
            this.channelId = channelId;
        }

        public NioSelector getNioSelector() {
            return nioSelector;
        }

        public void setNioSelector(NioSelector nioSelector) {
            this.nioSelector = nioSelector;
        }

        public short getApiKey() {
            return apiKey;
        }

        public void setApiKey(short apiKey) {
            this.apiKey = apiKey;
        }

        public short getApiVersion() {
            return apiVersion;
        }

        public void setApiVersion(short apiVersion) {
            this.apiVersion = apiVersion;
        }

        public byte getMessageFormat() {
            return messageFormat;
        }

        public void setMessageFormat(byte messageFormat) {
            this.messageFormat = messageFormat;
        }
    }

    public static class BaseMessageBytesEvent extends BaseHeader
    {
        private byte[] messageBytes;

        public byte[] getMessageBytes() {
            return messageBytes;
        }

        public void setMessageBytes(byte[] messageBytes) {
            this.messageBytes = messageBytes;
        }

        public static final EventFactory<BaseMessageBytesEvent> FACTORY = BaseMessageBytesEvent::new;
    }

    public static class BaseMessageBytesEventTranslator extends BaseMessageBytesEvent implements EventTranslator<BaseMessageBytesEvent>
    {
        @Override
        public void translateTo(BaseMessageBytesEvent baseMessageBytesEvent, long l) {
            baseMessageBytesEvent.setChannelId(this.getChannelId());
            baseMessageBytesEvent.setNioSelector(this.getNioSelector());
            baseMessageBytesEvent.setApiKey(this.getApiKey());
            baseMessageBytesEvent.setApiVersion(this.getApiVersion());
            baseMessageBytesEvent.setMessageFormat(this.getMessageFormat());
            baseMessageBytesEvent.setMessageBytes(this.getMessageBytes());
        }
    }

    public static class BaseMessageEvent extends BaseHeader
    {
        private GenericRecord genericRecord;

        public GenericRecord getGenericRecord() {
            return genericRecord;
        }

        public void setGenericRecord(GenericRecord genericRecord) {
            this.genericRecord = genericRecord;
        }

        public static final EventFactory<BaseMessageEvent> FACTORY = BaseMessageEvent::new;
    }


    public static class BaseMessageEventTranslator extends BaseMessageEvent implements EventTranslator<BaseMessageEvent>
    {
        @Override
        public void translateTo(BaseMessageEvent baseMessageEvent, long l) {
            baseMessageEvent.setChannelId(this.getChannelId());
            baseMessageEvent.setNioSelector(this.getNioSelector());
            baseMessageEvent.setApiKey(this.getApiKey());
            baseMessageEvent.setApiVersion(this.getApiVersion());
            baseMessageEvent.setMessageFormat(this.getMessageFormat());
            baseMessageEvent.setGenericRecord(this.getGenericRecord());
        }
    }

}
