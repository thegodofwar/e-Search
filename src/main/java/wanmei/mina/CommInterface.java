package wanmei.mina;

import java.io.UnsupportedEncodingException;
import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import outpost.service.DataBuffer;


/**
 * The communication interface between mina master and mina slave.
 * 
 * The data format for processing from master to slave and backward are the 
 * same, i.e.
 *   <serial(int)>
 *   <size(int)>
 *   <bytes...>
 *   
 * The data format for heartbeating from slave to master is
 *   <nBytes(int)>
 *   <bytes...>
 *   <port(int)>
 *   <type(int)>
 *   <slice(int)>
 *   <version(int)>
 *   <ttl(int)>
 * 
 * @author David
 *
 */
public class CommInterface {
    /**
     * The data structure containing the data and the serial number on the 
     * master.
     * 
     * @author David
     *
     */
    public static class DataSerial {
    	/**
    	 * switch (search_delete_tag){
    	 *    case 1: search operation;break;
    	 *    case 0: delete operation;break;
    	 *  }
    	 */
    	public int search_delete_tag;
        /**
         * the serial number
         */
        public int serial;
        /**
         * the data to be sent/received in the message
         */
        public DataBuffer data;
    }
    
    static ProcessProtocolEncoder PROCESS_PROTOCOL_ENCODER = 
        new ProcessProtocolEncoder();
    
    /**
     * The encoder class that encodes a DataSerial into bytes.
     * 
     * @author David
     *
     */
    public static class ProcessProtocolEncoder implements ProtocolEncoder {
        public void dispose(IoSession session) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void encode(IoSession session, Object message,
                ProtocolEncoderOutput out) throws Exception {
            DataSerial dataSerial = (DataSerial) message;
            /*
             * Allocate byte-buf
             */
            ByteBuffer byteBuf = ByteBuffer.allocate(
                    4 + 4 + 4 + dataSerial.data.size(), false);
            /*
             * Convert into bytes
             */
            byteBuf.putInt(dataSerial.search_delete_tag);
            byteBuf.putInt(dataSerial.serial);
            byteBuf.putInt(dataSerial.data.size());
            byteBuf.put(dataSerial.data.getData(), 0, dataSerial.data.size());
            
            byteBuf.flip();
            /*
             * write out
             */
            out.write(byteBuf);
        }
    }
    
    /**
     * The decoder class that decodes DataSerial objects from bytes.
     * 
     * @author David
     *
     */
    public static class ProcessMinaProtocolDecoder 
            extends CumulativeProtocolDecoder {
        /**
         * The default constructor.
         */
        protected ProcessMinaProtocolDecoder() {
            super();
        }

        @Override
        protected boolean doDecode(IoSession session, ByteBuffer in,
                ProtocolDecoderOutput out) throws Exception {
            if (in.remaining() < 12)
                return false;
            // remember position before reading
            int pos = in.position();
            /*
             * read serial and size
             */
            int search_delete_tag = in.getInt();
            int serial = in.getInt();
            int size = in.getInt();
            if (in.remaining() < size) {
                // rewind to last position
                in.position(pos);
                return false;
            } // if
            /*
             * Read data
             */
            DataSerial dataSerial = new DataSerial();
            dataSerial.search_delete_tag = search_delete_tag;
            dataSerial.serial = serial;
            dataSerial.data = new DataBuffer();
            dataSerial.data.setSize(size);
            in.get(dataSerial.data.getData(), 0, size);
            /*
             * Output dataSerial
             */
            out.write(dataSerial);
            
            return true;
        }
    }

    /**
     * A process protocol codec factory for both the master and the slave.
     */
    public static ProtocolCodecFactory PROCESS_PROTOCOL_CODEC_FACT = 
        new ProtocolCodecFactory() {
            public ProtocolDecoder getDecoder() throws Exception {
                return new ProcessMinaProtocolDecoder();
            }

            public ProtocolEncoder getEncoder() throws Exception {
                return PROCESS_PROTOCOL_ENCODER;
            }
        };

    /**
     * The data-structure for information of a heartbeat
     * 
     * Fields:
     *   host     String  the host of the slave
     *   port     int     the port of the slave
     *   type     int     the user-defined type of service
     *   slice    int     the slice of the slave
     *   version  int     the version of the slave. The higher versio means a 
     *                    newer version.
     *   ttl      int     the time-to-live in milli-seconds for this heartbeat. 
     *                    If next heartbeat does not arrive whithin this time 
     *                    period, the slave will be marked as dead.
     *             
     * @author David
     *
     */
    public static class HeartbeatInfo {
        public String host;
        public int port;
        public int type;
        public int slice;
        public int version;
        public int ttl;
        
        @Override
        public String toString() {
            return host + ":" + port + " - type = " + type + ", slice = "
                + slice + ", version = " + version + ", ttl = " + ttl;
        }
    }
    static HeartbeatProtocolEncoder HEARTBEAT_PROTOCOL_ENCODER = 
        new HeartbeatProtocolEncoder();
    /**
     * The encoder class that encodes a HeartbeatInfo into bytes.
     * 
     * @author David
     *
     */
    public static class HeartbeatProtocolEncoder implements ProtocolEncoder {
        public void dispose(IoSession session) throws Exception {
        }

        public void encode(IoSession session, Object message,
                ProtocolEncoderOutput out) throws Exception {
            HeartbeatInfo info = (HeartbeatInfo) message;
            byte[] host;
            try {
                host = info.host.getBytes("utf-8");
                
                ByteBuffer byteBuf = ByteBuffer.allocate(
                        4 + host.length + 4*5, false);

                byteBuf.putInt(host.length);
                byteBuf.put(host);
                byteBuf.putInt(info.port);
                byteBuf.putInt(info.type);
                byteBuf.putInt(info.slice);
                byteBuf.putInt(info.version);
                byteBuf.putInt(info.ttl);
                
                byteBuf.flip();
                
                out.write(byteBuf);
            } catch (UnsupportedEncodingException e) {
                // impossible
                e.printStackTrace();
            }
        }
    }
    
    /**
     * The decoder class that decode HeartbeatInfo objects from bytes.
     * 
     * @author David
     *
     */
    public static class HeartbeatProtocolDecoder 
            extends CumulativeProtocolDecoder {
        /**
         * The default constructor.
         */
        protected HeartbeatProtocolDecoder() {
            super();
        }

        @Override
        protected boolean doDecode(IoSession session, ByteBuffer in,
                ProtocolDecoderOutput out) throws Exception {
            if (in.remaining() < (4 * 6))
                return false;
            int orgPos = in.position();
            int hostLen = in.getInt();
            if (in.remaining() < (hostLen + 4*5)) {
                in.position(orgPos);
                return false;
            } // if
            HeartbeatInfo info = new HeartbeatInfo();
            byte[] host = new byte[hostLen];
            in.get(host);
            try {
                info.host = new String(host, "utf-8");
            } catch (UnsupportedEncodingException e) {
                // impossible
                e.printStackTrace();
            }
            
            info.port = in.getInt();
            info.type = in.getInt();
            info.slice = in.getInt();
            info.version = in.getInt();
            info.ttl = in.getInt();
            
            out.write(info);
            
            return true;
        }
    }
    /**
     * A heartbeat protocol codec factory for the heartbeat from slave to 
     * master.
     */
    public static ProtocolCodecFactory HEARTBEAT_PROTOCOL_CODEC_FACT = 
        new ProtocolCodecFactory() {
            public ProtocolDecoder getDecoder() throws Exception {
                return new HeartbeatProtocolDecoder();
            }

            public ProtocolEncoder getEncoder() throws Exception {
                return HEARTBEAT_PROTOCOL_ENCODER;
            }
        };
}

