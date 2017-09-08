package io.shunters.coda.processor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

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

    public static class RequestBytesEvent extends BaseHeader
    {
        private byte[] messageBytes;

        public byte[] getMessageBytes() {
            return messageBytes;
        }

        public void setMessageBytes(byte[] messageBytes) {
            this.messageBytes = messageBytes;
        }

        public static final EventFactory<RequestBytesEvent> FACTORY = RequestBytesEvent::new;
    }

    public static class RequestBytesEventTranslator extends RequestBytesEvent implements EventTranslator<RequestBytesEvent>
    {
        @Override
        public void translateTo(RequestBytesEvent baseMessageBytesEvent, long l) {
            baseMessageBytesEvent.setChannelId(this.getChannelId());
            baseMessageBytesEvent.setNioSelector(this.getNioSelector());
            baseMessageBytesEvent.setApiKey(this.getApiKey());
            baseMessageBytesEvent.setApiVersion(this.getApiVersion());
            baseMessageBytesEvent.setMessageFormat(this.getMessageFormat());
            baseMessageBytesEvent.setMessageBytes(this.getMessageBytes());
        }
    }

    public static class RequestEvent extends BaseHeader
    {
        private GenericRecord genericRecord;

        public GenericRecord getGenericRecord() {
            return genericRecord;
        }

        public void setGenericRecord(GenericRecord genericRecord) {
            this.genericRecord = genericRecord;
        }

        public static final EventFactory<RequestEvent> FACTORY = RequestEvent::new;
    }


    public static class RequestEventTranslator extends RequestEvent implements EventTranslator<RequestEvent>
    {
        @Override
        public void translateTo(RequestEvent baseMessageEvent, long l) {
            baseMessageEvent.setChannelId(this.getChannelId());
            baseMessageEvent.setNioSelector(this.getNioSelector());
            baseMessageEvent.setApiKey(this.getApiKey());
            baseMessageEvent.setApiVersion(this.getApiVersion());
            baseMessageEvent.setMessageFormat(this.getMessageFormat());
            baseMessageEvent.setGenericRecord(this.getGenericRecord());
        }
    }

    public static class ResponseEvent {

        private String channelId;
        private NioSelector nioSelector;

        private ByteBuffer responseBuffer;

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

        public ByteBuffer getResponseBuffer() {
            return responseBuffer;
        }

        public void setResponseBuffer(ByteBuffer responseBuffer) {
            this.responseBuffer = responseBuffer;
        }

        public static final EventFactory<ResponseEvent> FACTORY = ResponseEvent::new;
    }

    public static class ResponseEventTranslator extends ResponseEvent implements EventTranslator<ResponseEvent>
    {

        @Override
        public void translateTo(ResponseEvent responseEvent, long l) {
            responseEvent.setChannelId(this.getChannelId());
            responseEvent.setNioSelector(this.getNioSelector());
            responseEvent.setResponseBuffer(this.getResponseBuffer());
        }
    }


}
