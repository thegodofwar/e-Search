package wanmei.is.crawl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.RuntimeIOException;
import org.apache.mina.common.ThreadModel;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketConnector;

import outpost.service.ServiceMaster;
import outpost.service.mina.CommInterface;
import outpost.service.mina.CommInterface.DataSerial;
import outpost.service.mina.CommInterface.HeartbeatInfo;
import pf.is.data.EmlMeta;
import pf.is.data.LogMeta;
import pf.is.data.MailMeta;
import pf.is.parser.MailParser;
import pf.is.tool.MailDataParseTool;
import pf.mina.handler.LocalIndexSearcher;
import pf.utils.PartationUtil;
import pf.utils.index.IndexBuilder;

/**
 * The mina-based service-master. The slave must be a MinaServiceSlave.
 *  
 * @author liufukun
 *
 */
public class MinaLogMaster extends ServiceMaster 
        implements IoHandler {
	
	public static final Logger LOG=Logger.getLogger(MinaLogMaster.class.getName());
    
	protected LocalIndexSearcher searcher;
	
    //TODO:
//    private static final String MTAPATHSTR = "/data1/mtadata/";
//    private static String BASEPATHSTR = "/data1/is";
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
            LOG.error("Exception caught on session " + session, 
                    cause);
        }

        public void messageReceived(IoSession session, Object message)
                throws Exception {
            DataSerial dataSerial = (DataSerial) message; 
            if (isDebugging) {
                LOG.info("Message received in datasession ... serial: " 
                        + ((DataSerial) message).serial + ", slave: " + this);
            } // if
            // Process this response
            ProcessEntry entry = entries.get(dataSerial.serial);
            if (entry != null) {
                if (isDebugging) {
                    LOG.info("Entry for serial " + dataSerial.serial +
                            " found. slave: " + this + ", index: " + 
                            entry.index);
                } // if
                boolean isTimeout = System.currentTimeMillis() > entry.timeout;
                if (!isTimeout) {
                    entry.resArr[entry.index] = dataSerial.data;
                    entry.semaphore.release();
                } else {
                    LOG.info("Entry for serial" + dataSerial.serial +
                            " timeout. slave: " + this + ", index: " + 
                            entry.index);
                } // else
                entries.remove(dataSerial.serial);
                if (isDebugging) {
                    LOG.info("A ProcessEntry is cleared in messageReceived. " +
                    		"serial: " + ((MiniProcessEntry) entry).serial);
                } // if
            } else {
                LOG.info("Entry for serial " + dataSerial.serial +
                        " NOT found. slave: " + this);
            } // else
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
            	MinaLogMaster.this.removeSlave(this);
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
    
    public void setSearcher(LocalIndexSearcher searcher) {
    	this.searcher=searcher;
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
        	LOG.error("",e);
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
    public MinaLogMaster(int port, int slice) throws IOException {
        this(port, 8, 8, slice);
    }
    
    int lsNum = 1;
    
    public int getLsNum() {
		return lsNum;
	}

	public void setLsNum(int lsNum) {
		this.lsNum = lsNum;
	}

	/**
     * The constructor.
     * 
     * @param port  the port of the master's heartbeat listening socket
     * @param acceptorCount  the number of acceptors
     * @param connectorCount the number of connectors 
     * @throws IOException  if an I/O error occurs
     */
    public MinaLogMaster(int port, int acceptorCount, int connectorCount, int slice) throws IOException {
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
        this.slice = slice;
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
    
    AtomicInteger serialGenerator = new AtomicInteger();
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
    
    @Override
    protected ProcessEntry generateProcessEntry(long timeout) {
        MiniProcessEntry entry = new MiniProcessEntry();
        entry.timeout = timeout;
        entry.serial = serialGenerator.incrementAndGet();
        if (isDebugging) {
            LOG.info("An MinaProcessEntry was generated. serial: " 
                    + entry.serial);
        } // if
        return entry;
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
        dataSerial.data = entry.in;
        dataSerial.serial = minaEntry.serial;
        if (isDebugging) {
            LOG.info("Writing to session in asyncProcess... serial: " 
                    + minaEntry.serial + ", res: " + entry.resArr + ", index: " 
                    + entry.index + ", slave: " + entry.slave);
        } // if
        slave.session.write(dataSerial);
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

    //private List<Directory> indexes = new LinkedList<Directory>();
    private Queue<Directory> indexes = new ConcurrentLinkedQueue<Directory>();
    public List<Directory> removeIndexes() {
    	List<Directory> result = new ArrayList<Directory>();
    	int size = indexes.size();
    	LOG.info("[INFO] action=remove_index type=realtime size="+size);
    	while(indexes.size()>0) {
    		result.add(indexes.remove());
    		LOG.info("[INFO] action=add_index type=realtime size="+indexes.size());
    	}
    	
    	return result;
    }
    
//    private PooledMailParser parser = new PooledMailParser();
    private int slice;
    public void messageReceived(IoSession session, Object message)
            throws Exception {
        /*HeartbeatInfo info = (HeartbeatInfo) message;
        this.heartbeat(info.host, info.port, info.type, info.slice, 
                info.version, info.ttl);*/
    	HeartbeatInfo msg = (HeartbeatInfo) message;
        
        
        if (msg.host!= null && msg.host.length() > 0) {
        	String[] lines = msg.host.split("\\|\\~\\|");
        	Directory dir = new RAMDirectory();
	    	IndexBuilder builder = new IndexBuilder();
	    	builder.prepareByDir(dir);
	    	
	    	//create segment
			/*Path basePath = new Path(BASEPATHSTR);
			String day = DateUtil.genCurrentDayStr();
			Path dayPath = basePath.cat(MailDataParseTool.SEGMENT).cat(MailDataParseTool.ADD).cat(day);
			PathUtil.mkdirs(dayPath);
			Segments segs = new Segments(dayPath);
			int newseg = segs.createNewSegment();
			Path segPath = dayPath.cat(newseg+"");*/
	    	
	        for (String line : lines) {
	        	//TODO:index
//LOG.info("Received line:"+line);
	        	LogMeta msslog = MailDataParseTool.parseLog(line,searcher);

	        	if (msslog != null) {
	        		long usrid = msslog.getUsrid();
	        		int isNo = PartationUtil.getIsId(""+usrid, lsNum);
//	        		LOG.info("usrid:"+usrid +" isNo:"+isNo+" slice:"+slice);
	        		if (slice == isNo) {
//		        		String msgid = msslog.getMsgid();
		        		
		        		EmlMeta eml = null;
		        		/*if (PathUtil.exists(mtaPath)) {
		        			LOG.info("receive emlPath:"+mtaPath.getAbsolutePath());
		        			eml = parser.parse(mtaPath.getAbsolutePath(), false);
		        			PathUtil.delete(mtaPath);
		        		} else {*/
		        			eml = MailParser.parseMessage(msslog.getId(), msslog.isIspam());
//						}
		        			
		        			
		        		if (eml != null) {
//		        			LOG.info("eml:"+eml);
		        			MailMeta mailMeta = new MailMeta(eml.getSubject(), eml.getTxtBody()+" "+ eml.getHtmlBody(), eml.getFrom(), eml.getTo(), msslog.getId(), msslog.getTime(), msslog.getUsrid(), msslog.getFolder(), eml.getAttaches(), msslog.getDocid());
		        			builder.setIndexInfo(mailMeta.getDocid(), mailMeta);
		        			builder.index();
		        			
//		        			XMLUtil.writeConfig(mailMeta.getUsrid()+"", segPath.cat(mailMeta.getUsrid()+"_" +System.currentTimeMillis()).asFile(), mailMeta);
		        		} else {
		        			LOG.info("[ERROR] action=read_eml info=eml_null");
		        		}
	        		}
	        		
	        	}
	        }
	        builder.finish();
	        indexes.add(dir);
//	        PathUtil.mkdirs(segPath.cat(MailConstants.SEGFINISHED));
	        
        }
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
    
    public static void main(String[] args) {
    	try {
			@SuppressWarnings("unused")
			 
			MinaLogMaster master = new MinaLogMaster(5001, 8, 150, 0);
			int[] serverPorts = {5001};
			String[] serverHosts = {"127.0.0.1"};
			int slavePort = 8001;
			int heartBeatInterval = 30000;
			@SuppressWarnings("unused")
			MinaLogSlave slave = new MinaLogSlave(null, serverHosts, serverPorts,
	                InetAddress.getLocalHost().getHostName(), slavePort,
	                0, 0, 0, heartBeatInterval, 15, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
    }
}