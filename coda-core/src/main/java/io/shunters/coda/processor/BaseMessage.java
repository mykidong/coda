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

        public static final EventFactory<BaseMessageBytesEvent> FACTORY = new EventFactory<BaseMessageBytesEvent>() {
            @Override
            public BaseMessageBytesEvent newInstance() {
                return new BaseMessageBytesEvent();
            }
        };
    }

    public static class BaseMessageBytesEventTranslator implements EventTranslator<BaseMessageBytesEvent>
    {
        private String channelId;
        private NioSelector nioSelector;

        private short apiKey;
        private short apiVersion;
        private byte messageFormat;
        private byte[] messageBytes;

        public void setChannelId(String channelId) {
            this.channelId = channelId;
        }

        public void setNioSelector(NioSelector nioSelector) {
            this.nioSelector = nioSelector;
        }

        public void setApiKey(short apiKey) {
            this.apiKey = apiKey;
        }

        public void setApiVersion(short apiVersion) {
            this.apiVersion = apiVersion;
        }

        public void setMessageFormat(byte messageFormat) {
            this.messageFormat = messageFormat;
        }

        public void setMessageBytes(byte[] messageBytes) {
            this.messageBytes = messageBytes;
        }

        @Override
        public void translateTo(BaseMessageBytesEvent baseMessageBytesEvent, long l) {
            baseMessageBytesEvent.setChannelId(this.channelId);
            baseMessageBytesEvent.setNioSelector(this.nioSelector);
            baseMessageBytesEvent.setApiKey(this.apiKey);
            baseMessageBytesEvent.setApiVersion(this.apiVersion);
            baseMessageBytesEvent.setMessageFormat(this.messageFormat);
            baseMessageBytesEvent.setMessageBytes(this.messageBytes);
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

        public static final EventFactory<BaseMessageEvent> FACTORY = new EventFactory<BaseMessageEvent>() {
            @Override
            public BaseMessageEvent newInstance() {
                return new BaseMessageEvent();
            }
        };
    }


    public static class BaseMessageEventTranslator implements EventTranslator<BaseMessageEvent>
    {
        private String channelId;
        private NioSelector nioSelector;

        private short apiKey;
        private short apiVersion;
        private byte messageFormat;
        private GenericRecord genericRecord;

        public void setChannelId(String channelId) {
            this.channelId = channelId;
        }

        public void setNioSelector(NioSelector nioSelector) {
            this.nioSelector = nioSelector;
        }

        public void setApiKey(short apiKey) {
            this.apiKey = apiKey;
        }

        public void setApiVersion(short apiVersion) {
            this.apiVersion = apiVersion;
        }

        public void setMessageFormat(byte messageFormat) {
            this.messageFormat = messageFormat;
        }

        public void setGenericRecord(GenericRecord genericRecord) {
            this.genericRecord = genericRecord;
        }

        @Override
        public void translateTo(BaseMessageEvent baseMessageEvent, long l) {
            baseMessageEvent.setChannelId(this.channelId);
            baseMessageEvent.setNioSelector(this.nioSelector);
            baseMessageEvent.setApiKey(this.apiKey);
            baseMessageEvent.setApiVersion(this.apiVersion);
            baseMessageEvent.setMessageFormat(this.messageFormat);
            baseMessageEvent.setGenericRecord(this.genericRecord);
        }
    }

}
