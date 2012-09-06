package pf.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.RuntimeIOException;
import org.apache.mina.common.ThreadModel;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketConnector;

import pf.mina.CommInterface.DataSerial;
import pf.mina.CommInterface.HeartbeatInfo;
import pf.mina.ServiceMaster.DeleteProcessEntry;

/**
 * The mina-based service-master. The slave must be a MinaServiceSlave.
 *  
 * @author liufukun
 *
 */
public class MinaServiceMaster extends ServiceMaster 
        implements IoHandler {
	public static final Logger LOG=Logger.getLogger(MinaServiceMaster.class.getName());    
    /**
     * The slave-info data-structure, which is inherited from SalveInfo.
     * Fields:
     *   session  ProtocolSession  the mina-session for this slave 
     * 
     * @author David
     *
     */
    public class MinaSlaveInfo extends SlaveInfo 
            implements IoHandler {
        IoSession session;
        
        /* *************************
         * ProtocolHandler interface
         ****************************/
        public void exceptionCaught(IoSession session, Throwable cause)
                throws Exception {
            LOG.error("Exception caught on session " + session,cause);
        }

        public void messageReceived(IoSession session, Object message)
                throws Exception {
            DataSerial dataSerial = (DataSerial) message; 
            if (isDebugging) {
                LOG.info("Message received in datasession ... search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+" serial: " 
                        + ((DataSerial) message).serial + ", slave: " + this);
            } // if
            // Process this response
            if(dataSerial.search_delete_tag==1) {
            	ProcessEntry entry = entries.get(dataSerial.serial);
                if (entry != null) {
                    if (isDebugging) {
                        LOG.info("Entry for search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+" serial " + dataSerial.serial +
                                " found. slave: " + this + ", index: " + 
                                entry.index);
                    } // if
                    boolean isTimeout = System.currentTimeMillis() > entry.timeout;
                    if (!isTimeout) {
                        entry.resArr[entry.index] = dataSerial.data;
                        entry.semaphore.release();
                    } else {
                        LOG.info("Entry for search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+" serial" + dataSerial.serial +
                                " timeout. slave: " + this + ", index: " + 
                                entry.index);
                    } // else
                    entries.remove(dataSerial.serial);
                    if (isDebugging) {
                        LOG.info("A ProcessEntry is cleared in messageReceived. search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+
                        		" serial: " + ((MiniProcessEntry) entry).serial);
                    } // if
                } else {
                    LOG.info("Entry for search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+" serial " + dataSerial.serial +
                            " NOT found. slave: " + this);
                } // else	
            } else if(dataSerial.search_delete_tag==0) {
            	DeleteProcessEntry deleteEntry = deleteEntries.get(dataSerial.serial);
            	if(deleteEntry != null) {
            		if(isDebugging) {
            			LOG.info("Entry for search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+" serial " + dataSerial.serial +
                                " found. slave: " + this + ", index: " + 
                                deleteEntry.index);
            		}//if
            		boolean isTimeout = System.currentTimeMillis() > deleteEntry.timeout;
            		if (!isTimeout) {
                        deleteEntry.delResult[deleteEntry.index] = dataSerial.data;
                        deleteEntry.semaphore.release();
                    } else {
                        LOG.info("Entry for search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+" serial" + dataSerial.serial +
                                " timeout. slave: " + this + ", index: " + 
                                deleteEntry.index);
                    } // else
                    deleteEntries.remove(dataSerial.serial);
                    if (isDebugging) {
                        LOG.info("A ProcessEntry is cleared in messageReceived. search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+
                        		" serial: " + ((MiniDeleteProcessEntry) deleteEntry).serial);
                    } // if
            	} else {
                    LOG.info("Entry for search_delete_tag="+(dataSerial.search_delete_tag==1?"search":"delete")+" serial " + dataSerial.serial +
                            " NOT found. slave: " + this);
                } // else	
            }
            
        }

        public void messageSent(IoSession session, Object message)
                throws Exception {
            if (isDebugging) {
                LOG.info("Message sent in data session... serial: " 
                        + ((DataSerial) message).serial + ", slave: " + this);
            } // if
        }
        public void sessionClosed(IoSession session) throws Exception {
            LOG.info("Data session of slave(" + this + ") closed!");
            if (!isDestroying) {
                MinaServiceMaster.this.removeSlave(this);
            } // if
        }
        public void sessionCreated(IoSession session) throws Exception {
        }
        public void sessionIdle(IoSession session, IdleStatus status)
                throws Exception {
        }
        public void sessionOpened(IoSession session) throws Exception {
        }
        
    }

    @Override
    protected SlaveInfo generateSlaveInfo(String host, int port, int type) {
        MinaSlaveInfo slave = new MinaSlaveInfo();
        
        ConnectFuture conn = connector.connect(new InetSocketAddress(host, port),
                slave);
        conn.join();

        try {
            slave.session = conn.getSession();
        } catch (RuntimeIOException e) {
            e.printStackTrace();
            return null;
        }
        
        return slave;
    }
    
    /**
     * The acceptor for receiving heartbeat
     */
    SocketAcceptor acceptor;
    /**
     * The connector for sending request
     */
    SocketConnector connector;
    
    /**
     * THe constructor with default acceptorCount(8) and connectorCount(8) values.
     * 
     * @param port  the port of the master's heartbeat listening socket
     * @throws IOException  if an I/O error occurs
     */
    public MinaServiceMaster(int port) throws IOException {
        this(port, 8, 8);
    }
    /**
     * The constructor.
     * 
     * @param port  the port of the master's heartbeat listening socket
     * @param acceptorCount  the number of acceptors
     * @param connectorCount the number of connectors 
     * @throws IOException  if an I/O error occurs
     */
    public MinaServiceMaster(int port, int acceptorCount, int connectorCount) throws IOException {
        acceptor = new SocketAcceptor(acceptorCount, Executors.newCachedThreadPool());
        acceptor.getDefaultConfig().setThreadModel(ThreadModel.MANUAL);
        acceptor.getDefaultConfig().setReuseAddress(true);
        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(
                CommInterface.HEARTBEAT_PROTOCOL_CODEC_FACT));
        acceptor.bind(new InetSocketAddress(port), this);
        
        connector = new SocketConnector(connectorCount, Executors.newCachedThreadPool());
        connector.getDefaultConfig().setThreadModel(ThreadModel.MANUAL);
        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(
            CommInterface.PROCESS_PROTOCOL_CODEC_FACT));
    }
    
    /**
     * Stop the master.
     */
    public void stop() {
        acceptor.unbindAll();
/* TODO stop all slaves
        SlavesInfo[] allSlaves;
        synchronized (slaves) {
            allSlaves = slaves.values().toArray(new SlavesInfo[slaves.size()]);
        }
        
        for (SlavesInfo sliceSlaves: allSlaves) {
            for (SlaveInfo slave: sliceSlaves.slaves) {
                ((MinaSlaveInfo) slave).session.close();
            } // for slave
        } // for allSlaves
*/        
    }
    
    /**
     * the prosessing entries as a serial -> ProcessEntry map.
     */
    Map<Integer, ProcessEntry> entries = 
        new ConcurrentHashMap<Integer, ProcessEntry>();
    
    /**
     * the serial number refers to search
     */
    AtomicInteger serialGenerator = new AtomicInteger();
    
    /**
     * the Map with entries for processing deleting index according to mskey
     */
    Map<Integer, DeleteProcessEntry> deleteEntries = 
    	new ConcurrentHashMap<Integer, DeleteProcessEntry>();
    
    /**
     * the serial number refers to delete index
     */
    AtomicInteger deleteSerialGenerator = new AtomicInteger();
    
    /**
     * The ProcessEntry for mina server.
     * Fields (other than those in ProcessEntry):
     *   serial  int  the serial number of this entry.
     *   
     * @author david
     *
     */
    public static class MiniProcessEntry extends ProcessEntry {
        public int serial;
    }
    
    public static class MiniDeleteProcessEntry extends DeleteProcessEntry {
        public int serial;
    }
    
    @Override
    protected ProcessEntry generateProcessEntry(long timeout) {
        MiniProcessEntry entry = new MiniProcessEntry();
        entry.timeout = timeout;
        entry.serial = serialGenerator.incrementAndGet();
 //LOG.error("Liufukun search Test: "+entry.serial);
        if (isDebugging) {
            LOG.info("An MinaProcessEntry was generated. serial: " 
                    + entry.serial);
        } // if
        return entry;
    }
    
    @Override
    protected DeleteProcessEntry generateDeleteProcessEntry(long timeout) {
    	MiniDeleteProcessEntry deleteEntry = new MiniDeleteProcessEntry();
    	deleteEntry.timeout = timeout;
    	deleteEntry.serial = deleteSerialGenerator.incrementAndGet();
 //LOG.error("Liufukun delete Test: "+deleteEntry.serial);
    	if(isDebugging) {
    	  LOG.info("An MinaDeleteProcessEntry was generated. serial: "+deleteEntry.serial);
    	}
    	return deleteEntry;
    }
    
    @Override
    protected void internalAsyncProcess(ProcessEntry entry) {
        MiniProcessEntry minaEntry = (MiniProcessEntry) entry;
        entries.put(minaEntry.serial, entry);
        if (isDebugging) {
            LOG.info("A ProcessEntry is put in entries in asyncProcess. " +
            		"serial: " + ((MiniProcessEntry) entry).serial);
        } // if
        MinaSlaveInfo slave = (MinaSlaveInfo) entry.slave;
        
        DataSerial dataSerial = new DataSerial();
        dataSerial.search_delete_tag = 1;
        dataSerial.serial = minaEntry.serial;
        dataSerial.data = entry.in;
        if (isDebugging) {
            LOG.info("Writing to session in asyncProcess... serial: " 
                    + minaEntry.serial + ", res: " + minaEntry.resArr + ", index: " 
                    + minaEntry.index + ", slave: " + minaEntry.slave);
        } // if
        slave.session.write(dataSerial);
    }
    
    @Override
    protected void deleteInternalAsyncProcess(DeleteProcessEntry entry) {
    	MiniDeleteProcessEntry deleteMinaEntry = (MiniDeleteProcessEntry) entry;
    	deleteEntries.put(deleteMinaEntry.serial, deleteMinaEntry);
    	if(isDebugging) {
    	   LOG.info("A DeleteProcessEntry is put in deleteEntries in deleteAsyncProcess. "+
    			   "serial: "+ ((MiniDeleteProcessEntry)entry).serial);
    	}
    	MinaSlaveInfo slave = (MinaSlaveInfo) entry.slave;
    	DataSerial deleteDataSerial = new DataSerial();
    	deleteDataSerial.search_delete_tag = 0;
    	deleteDataSerial.serial = deleteMinaEntry.serial;
    	deleteDataSerial.data = entry.in;
        if(isDebugging) {
           LOG.info("Writing to session in deleteAsyncProcess... serial: "
        		   +deleteMinaEntry.serial + ", delResult: "+deleteMinaEntry.delResult + ", index: "
        		   +deleteMinaEntry.index + ", slave: " + deleteMinaEntry.slave);
        }
        slave.session.write(deleteDataSerial);
    }
    
    @Override
    protected void clearProcessEntry(ProcessEntry entry) {
        entries.remove(((MiniProcessEntry) entry).serial);
        if (isDebugging) {
            LOG.info("A ProcessEntry is cleared in clearProcessEntry. serial: " 
                    + ((MiniProcessEntry) entry).serial);
        } // if
    }
    
    @Override
    protected void clearDeleteProcessEntry(DeleteProcessEntry deleteEntry) {
    	deleteEntries.remove(((MiniDeleteProcessEntry) deleteEntry).serial);
    	if(isDebugging) {
    		LOG.info("A DeleteProcessEntry is cleared in clearProcessEntry. serial: "
    				+((MiniDeleteProcessEntry) deleteEntry).serial);
    	}
    }
    
    @Override
    protected void destroySlave(SlaveInfo slave) {
        MinaSlaveInfo mSlave = (MinaSlaveInfo) slave;
        if (!mSlave.session.isClosing()) {
            LOG.info("Closing data session of slave {" + slave + "}");
            mSlave.session.close();
        } // if
        
        super.destroySlave(slave);
    }
    
    /* *
     * Methods of ProtocolHandler interface as the heartbeat server
     */
    public void exceptionCaught(IoSession session, Throwable cause)
            throws Exception {
        LOG.error("Exception caught in heartbeat listener!", cause);
    }

    public void messageReceived(IoSession session, Object message)
            throws Exception {
        HeartbeatInfo info = (HeartbeatInfo) message;
        this.heartbeat(info.host, info.port, info.type, info.slice, 
                info.version, info.ttl);
    }

    public void messageSent(IoSession session, Object message) 
            throws Exception {
    }

    public void sessionClosed(IoSession session) throws Exception {
        LOG.info("Session closed in heartbeat listener!");
    }

    public void sessionCreated(IoSession session) throws Exception {
    }

    public void sessionIdle(IoSession session, IdleStatus status)
            throws Exception {
    }

    public void sessionOpened(IoSession session) throws Exception {
    }
}
