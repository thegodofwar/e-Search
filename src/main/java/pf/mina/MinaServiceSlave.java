package pf.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.ThreadModel;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketConnector;

import outpost.service.mina.MultiThreadFilter;

import pf.mina.CommInterface.DataSerial;
import pf.mina.CommInterface.HeartbeatInfo;
import pf.mina.handler.DeleteIHandler;
import pf.mina.handler.IHandler;

/**
 * The mina-based service-slave. The master must be a MinaServiceMaster.
 * 
 * Fields:
 *   version  int  the version of this slave. Higher version slave can suppress lower version slave.
 * 
 * @author liufukun
 *
 */
public class MinaServiceSlave implements IoHandler {
	public static final Logger LOG=Logger.getLogger(MinaServiceSlave.class.getName());
    
    protected boolean isDebugging = true;
    public void setDebugging(boolean vl) {
        this.isDebugging = vl;
    }
    
    IHandler handler;
    DeleteIHandler deleteHandler;
    SocketAcceptor acceptor;
    
    /**
     * The interval between two hearbeat
     */
    protected int heartbeatInterv;
    /**
     * The hearbeat thread
     */
    Thread[] heartbeats;
    
    private static SocketConnector connector;
    static {
        connector = new SocketConnector(1, Executors.newCachedThreadPool());
        connector.getDefaultConfig().setThreadModel(ThreadModel.MANUAL);
        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(
                        CommInterface.HEARTBEAT_PROTOCOL_CODEC_FACT));
    }
    
    protected HeartbeatInfo hbInfo = new HeartbeatInfo();
    
    public int getVersion() {
        return hbInfo.version;
    }
    public void setVersion(int version) {
        hbInfo.version = version;
    }
    
    protected InetSocketAddress[] masterAddrs;

    /**
     * The constructor with processorCount set to 5.
     * @param handler  the handler for handling each search processing request
     * @param deleteHandler  the deleteHandler for handling each delete processing request
     * @param serverHost  the host of the server
     * @param serverPort  the port of the server
     * @param slaveHost  the host of the slave
     * @param slavePort  the port of the slave
     * @param type  the user-defined type of service
     * @param slice  the slice number of this service
     * @param version  the version of this service
     * @param hbInter  the interval between two heartbeat
     * @throws IOException  if an I/O error occurs
     */
    public MinaServiceSlave(IHandler handler, DeleteIHandler deleteHandler,final String[] serverHosts, 
            final int[] serverPorts, final String slaveHost, 
            final int slavePort, final int type, final int slice, int version, 
            final int hbInter) throws IOException {
        this(handler, deleteHandler, serverHosts, serverPorts, slaveHost, slavePort, type, slice, version, hbInter, 
                5);
    }
    /**
     * The constructor.
     * @param handler  the handler for handling each processing request
     * @param deleteHandler  the deleteHandler for handling each delete processing request
     * @param serverHost  the host of the server
     * @param serverPort  the port of the server
     * @param slaveHost  the host of the slave
     * @param slavePort  the port of the slave
     * @param type  the user-defined type of service
     * @param slice  the slice number of this service
     * @param version  the version of this service
     * @param hbInter  the interval between two heartbeat
     * @param processorCount the number of threads for processing messages
     * @throws IOException  if an I/O error occurs
     */
    public MinaServiceSlave(IHandler handler, DeleteIHandler deleteHandler, final String[] serverHosts, 
            final int[] serverPorts, final String slaveHost, 
            final int slavePort, final int type, final int slice, int version, 
            final int hbInter, int processorCount) throws IOException {
        this.handler = handler;
        this.deleteHandler = deleteHandler;
        this.heartbeatInterv = hbInter;
        
        if (serverHosts.length != serverPorts.length) {
            throw new RuntimeException("The length of serverHosts is not " +
                    "equal to that of serverPorts!");
        } // if
        
        Executor executor = Executors.newFixedThreadPool(processorCount + 2);
        acceptor = new SocketAcceptor(2, executor);
        acceptor.getDefaultConfig().setThreadModel(ThreadModel.MANUAL);
        acceptor.getDefaultConfig().setReuseAddress(true);
        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(
                CommInterface.PROCESS_PROTOCOL_CODEC_FACT));
        if (processorCount > 1)
            acceptor.getFilterChain().addLast("multi-thread", new MultiThreadFilter(executor));
        acceptor.bind(new InetSocketAddress(slavePort), this);
        
        hbInfo.host = slaveHost;
        hbInfo.port = slavePort;
        hbInfo.type = type;
        hbInfo.slice = slice;
        hbInfo.ttl = heartbeatInterv * 3;
        hbInfo.version = version;
        
        heartbeats = new Thread[serverHosts.length];
        masterAddrs = new InetSocketAddress[serverHosts.length];
        for (int i = 0; i < heartbeats.length; i ++) {
            final int curIndex = i;
            masterAddrs[curIndex] = new InetSocketAddress(serverHosts[curIndex], 
                    serverPorts[curIndex]);
            heartbeats[i] = new Thread(
                    "Service slave (slice = " + slice + ") heart-beat!") {
                {
                    this.setDaemon(true);
                }
                IoSession master;
                public void run() {
                    master = null;
                    
                    IoHandlerAdapter handler = new IoHandlerAdapter() {
                        @Override
                        public void sessionClosed(IoSession session) 
                                throws Exception {
                            LOG.info("Heartbeat session closed!");
                            // set master to null so that session will
                            // be recreated.
                            master = null;
                        }
                    };

                    while (!Thread.interrupted()) {
                        if (master == null) {
                            try {
                                LOG.info("Connecting to " + 
                                        masterAddrs[curIndex] + " ... " +
                                        "slice: " + slice + ", type: " + type);
                                ConnectFuture conn = connector.connect(
                                        masterAddrs[curIndex], handler);
                                conn.join();
                                master = conn.getSession();
                                LOG.info("Connected with " + 
                                        masterAddrs[curIndex] + "! " +
                                        "slice: " + slice + ", type: " + type);
                            } catch (Exception e) {
                                LOG.error("Failed to connected " +
                                        "with " + masterAddrs[curIndex] + 
                                        "! slice: " + slice + ", type: " + type, 
                                        e);
                                // connecting failed, wait for next try
                            }
                        } // if
                        
                        if (master != null) {
                            master.write(hbInfo);
                            
                            LOG.info("Heartbeat to master " + 
                                    masterAddrs[curIndex] + ": " + hbInfo 
                                    + "!");
                        } // if
                        
                        try {
                            long start = System.currentTimeMillis();
                            while (true) {
                                long time;
                                if (master != null)
                                    time = heartbeatInterv;
                                else
                                    time = 5000;
                                long now = System.currentTimeMillis();
                                if (now - start >= time)
                                    break;
                                Thread.sleep(Math.min(5000, 
                                        time - (now - start)));
                            } // while
                        } catch (InterruptedException e) {
                            LOG.info(e.getMessage());
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } // while
                    
                    if (master != null) {
                        master.close().join();
                    } // if
                    
                    LOG.info("Heartbeat thread exits!");
                }
            };
            heartbeats[i].start();
        } // for i
    }
    
    /**
     * Heartbeat the masters with ttl == 0 to say good bye
     */
    protected void sayGoodByes() {
        LOG.info("Saying good byes ...");
        
        IoSession master;
        IoHandlerAdapter handler = new IoHandlerAdapter() {
            @Override
            public void sessionClosed(IoSession session) 
                    throws Exception {
                LOG.info("Heartbeat session closed!");
            }
        };
        hbInfo.ttl = 0;
        for (int curIndex = 0; curIndex < masterAddrs.length; curIndex ++) {
            try {
                LOG.info("Connecting to " + masterAddrs[curIndex] + " ... " + 
                        "slice: " + hbInfo.slice + ", type: " + hbInfo.type);
                ConnectFuture conn = connector.connect(masterAddrs[curIndex], 
                        handler);
                conn.join();
                master = conn.getSession();
                LOG.info("Connected with " + masterAddrs[curIndex] + "! " +
                         "slice: " + hbInfo.slice + ", type: " + hbInfo.type);
                
                master.write(hbInfo).join();
                
                LOG.info("Heartbeat to master " + masterAddrs[curIndex] + ": " +
                        hbInfo + "!");
                
                master.close().join();
            } catch (Exception e) {
                LOG.error("Failed to connected with " + 
                        masterAddrs[curIndex] + "! slice: " + hbInfo.slice + 
                        ", type: " + hbInfo.type, e);
                // connecting failed, ignore this master
            }
        } // for i
    }

    /**
     * Stop the slave. The slave will tell the master that he is dying by
     * heartbeating the master with ttl equals to 0.
     */
    public void stop() {
        /*
         * Stop the normal heartbeat threads
         */
        for (Thread thd: heartbeats)
            thd.interrupt();
        for (Thread thd: heartbeats) {
            try {
                thd.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        } // for thd
        /*
         * Unbind listening acceptor
         */
        acceptor.unbindAll();
        // say good byes to masters
        sayGoodByes();
        
        LOG.info("The slave was stopped!");
    }
    
    public void exceptionCaught(IoSession session, Throwable cause)
            throws Exception {
        LOG.error("exceptionCaught", cause);
    }
    public void messageReceived(IoSession session, Object message)
            throws Exception {
        DataSerial dataSerial = (DataSerial) message;
        if (isDebugging) {
            LOG.info("Message receivied search_delete_tag = "+(dataSerial.search_delete_tag==1?"search":"delete")+" serial: " + dataSerial.serial);
        } // if
        if(dataSerial.search_delete_tag==1) {
        	dataSerial.data = handler.exec(dataSerial.data);
        } else if(dataSerial.search_delete_tag==0) {
        	dataSerial.data = deleteHandler.exec(dataSerial.data);
        }
        if (isDebugging) {
            LOG.info("Sending back message search_delete_tag = "+(dataSerial.search_delete_tag==1?"search":"delete")+" serial: " + dataSerial.serial);
        } // if
        session.write(dataSerial);
    }
    public void messageSent(IoSession session, Object message) 
        throws Exception {
        if (isDebugging) {
            LOG.info("Message sent. serial: " + ((DataSerial) message).serial);
        } // if
    }
    public void sessionClosed(IoSession session) throws Exception {
        LOG.info("Data session " + session + " was closed!");
    }
    public void sessionCreated(IoSession session) throws Exception {
    }
    public void sessionIdle(IoSession session, IdleStatus status)
            throws Exception {
    }
    public void sessionOpened(IoSession session) throws Exception {
    }
}
