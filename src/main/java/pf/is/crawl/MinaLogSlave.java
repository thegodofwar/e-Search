package pf.is.crawl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

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

import outpost.service.IHandler;
import outpost.service.mina.CommInterface;
import outpost.service.mina.MultiThreadFilter;
import outpost.service.mina.CommInterface.DataSerial;
import outpost.service.mina.CommInterface.HeartbeatInfo;
import pf.is.data.LogMeta;
import pf.is.tool.MailDataParseTool;
import pf.utils.DateUtil;
import pf.utils.XMLUtil;
import org.apache.commons.lang.math.NumberUtils;


/**
 * The mina-based service-slave. The master must be a MinaServiceMaster.
 * 
 * Fields:
 *   version  int  the version of this slave. Higher version slave can suppress lower version slave.
 * 
 * @author liufukun
 *
 */
public class MinaLogSlave implements IoHandler {
	public static final Logger LOG=Logger.getLogger(MinaLogSlave.class.getName());
	
    protected boolean isDebugging = false;
    public void setDebugging(boolean vl) {
        this.isDebugging = vl;
    }
    
    IHandler handler;
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
     * @param handler  the handler for handling each processing request
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
    public MinaLogSlave(IHandler handler, final String[] serverHosts, 
            final int[] serverPorts, final String slaveHost, 
            final int slavePort, final int type, final int slice, int version, 
            final int hbInter) throws IOException {
        this(handler, serverHosts, serverPorts, slaveHost, slavePort, type, slice, version, hbInter, 
                5, false);
    }
    
    class SendLogsSlave implements Runnable {
    	public LinkedBlockingQueue<HeartbeatInfo> queue;
    	public IoSession master;
    	public InetSocketAddress masterAddress;
    	public int slice;
    	public int type;
    	
    	public SendLogsSlave(LinkedBlockingQueue<HeartbeatInfo> queue,InetSocketAddress masterAddress,int slice,int type) {
    		this.queue=queue;
    		this.masterAddress=masterAddress;
    		this.slice=slice;
    		this.type=type;
    	}
    	
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
             HeartbeatInfo exceptionSendInfo=null;
             while (true) {
              if (master == null) {
 					try {
 						LOG.info("Connecting to " + masterAddress + " ... "
 								+ "slice: " + slice + ", type: " + type);
 						ConnectFuture conn = connector.connect(masterAddress,
 								handler);
 						conn.join();
 						master = conn.getSession();
 						LOG.info("Connected with " + masterAddress + "! "
 								+ "slice: " + slice + ", type: " + type);
 						if(exceptionSendInfo!=null) {
 							master.write(exceptionSendInfo);
 							exceptionSendInfo=null;
 						}
 					} catch (Exception e) {
 						LOG.info("Failed to connect " + "with " + masterAddress
 								+ "! slice: " + slice + ", type: " + type, e);
 						// connecting failed, wait for next try
 						try {
 							Thread.sleep(3000);
 						} catch (InterruptedException e1) {
 							LOG.error("",e);
 						}
 						continue;
 					}
 			  }
              while (!Thread.interrupted()) {
				HeartbeatInfo sendInfo = null;
				try {
					sendInfo = queue.take();
				} catch (InterruptedException e) {
					LOG.error("", e);
				}
				try {
				   master.write(sendInfo);
				} catch (Exception e) {
				   LOG.error("Error occurs while flushing log message in slave to master",e);
				   exceptionSendInfo=sendInfo;
				   break;
				}
				LOG.info("FULLSEND to master " + masterAddress + ": "
						+ sendInfo.host.length() + "!");
			}
            if (master != null) {
                master.close().join();
            } // if
            LOG.info("Heartbeat thread exits!");
    	}
            
    	}
    }
    
    /**
     * The constructor.
     * @param handler  the handler for handling each processing request
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
    public MinaLogSlave(IHandler handler, final String[] serverHosts, 
            final int[] serverPorts, final String slaveHost, 
            final int slavePort, final int type, final int slice, int version, 
            final int hbInter, int processorCount, boolean isDebug) throws IOException {
        this.handler = handler;
        this.heartbeatInterv = hbInter;
        this.isDebugging = isDebug;
        
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
        
        masterAddrs = new InetSocketAddress[serverHosts.length];
        for(int k=0;k<masterAddrs.length;k++) {
        	masterAddrs[k] = new InetSocketAddress(serverHosts[k],serverPorts[k]);
        }
        LinkedBlockingQueue[] queues=new LinkedBlockingQueue[serverHosts.length];
        for(int i=0;i<queues.length;i++) {
        	queues[i]=new LinkedBlockingQueue<HeartbeatInfo>();
        }
        heartbeats = new Thread[serverHosts.length];
        for(int j=0;j<heartbeats.length;j++) {
        	heartbeats[j]=new Thread(new SendLogsSlave(queues[j],masterAddrs[j],slice,type));
        	heartbeats[j].setName("Service slave (slice = " + slice + ") heart-beat!");
        	heartbeats[j].setDaemon(true);
        	heartbeats[j].start();
        }
        
        String currentHour = "";
        RandomAccessFile randomFile = null;
        long lastTimeFileSize = 0;
        boolean isFirst = true;
                
        while (!Thread.interrupted()) {
			// generate info
			String nowHour = DateUtil.genCurrentHourStr();

			if (!currentHour.equals(nowHour)) {
				// need change file
				currentHour = nowHour;
				lastTimeFileSize = 0;
				// TODO:need to ensure the method of gen file name;

				String fileName = null;
				try {
					fileName = logPath + currentHour + "_"
							+ InetAddress.getLocalHost().getHostName()
							+ logName;
					LOG.info("[INFO] action=check current_file=" + fileName);

					if (randomFile != null) {
						randomFile.close();
					}
					randomFile = new RandomAccessFile(fileName, "r");

				} catch (FileNotFoundException e) {

					LOG.error("Current-hour log file [" + fileName
							+ "] has not created!");
					currentHour = "";

					try {
						Thread.sleep(3000);
						continue;
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						LOG.error("", e1);
					}

				} catch (IOException e) {
					LOG.error("", e);
				}
			}

			StringBuilder sendMsg = new StringBuilder("");

			try {
				if (isFirst) {
					lastTimeFileSize = 0;
					isFirst = false;
				}
				LOG.info("[INFO] action=seek filesize=" + lastTimeFileSize);
				randomFile.seek(lastTimeFileSize);
				String tmp = "";

				int count = 0;
				while ((tmp = randomFile.readLine()) != null) {
					if (!tmp.trim().startsWith("Jan")
							&& !tmp.trim().startsWith("Feb")
							&& !tmp.trim().startsWith("Mar")
							&& !tmp.trim().startsWith("Apr")
							&& !tmp.trim().startsWith("May")
							&& !tmp.trim().startsWith("Jun")
							&& !tmp.trim().startsWith("Jul")
							&& !tmp.trim().startsWith("Aug")
							&& !tmp.trim().startsWith("Sep")
							&& !tmp.trim().startsWith("Oct")
							&& !tmp.trim().startsWith("Nov")
							&& !tmp.trim().startsWith("Dec")) {
						if (tmp.length() != 0)
							LOG.error("Bad line:" + tmp);
						continue;
					}
					count++;

					if (count > 10000) {
						if (sendMsg.length() > 0) {
							hbInfo.host = sendMsg.toString();
						} else {
							hbInfo.host = "";
						}
						for(int i=0;i<queues.length;i++) {
							try {
								queues[i].put(hbInfo);
							} catch (InterruptedException e) {
								LOG.error("",e);
							}
						}
						sendMsg = new StringBuilder("");
						count = 0;
					}
					
					if (tmp.indexOf("173search") < 0) {
						continue;
					}

					LogMeta logmeta = MailDataParseTool.parseLog(tmp);

					if (logmeta != null) {
						sendMsg.append(tmp).append("|~|");
					}

				}
				lastTimeFileSize = randomFile.length();
			} catch (IOException e1) {
				LOG.error("", e1);
			}
			
			if (sendMsg.length() > 0) {
        		hbInfo.host = sendMsg.toString();
        	  } else {
        		hbInfo.host = "";
        	}
			for(int i=0;i<queues.length;i++) {
				try {
					queues[i].put(hbInfo);
				} catch (InterruptedException e) {
					LOG.error("",e);
				}
			}
			
			try {
				long start = System.currentTimeMillis();
				while (true) {
					long time;
					time = heartbeatInterv;
					long now = System.currentTimeMillis();
					if (now - start >= time)
						break;
					Thread.sleep(Math.min(3000, time - (now - start)));
				} // while
			} catch (InterruptedException e) {
				LOG.info(e.getMessage());
				Thread.currentThread().interrupt();
				break;
			}
		} // while

	}
    
    /**
     * The constructor.
     * @param handler  the handler for handling each processing request
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
    public MinaLogSlave(IHandler handler, final String[] serverHosts, 
            final int[] serverPorts, final String slaveHost, 
            final int slavePort, final int type, final int slice, int version, 
            final int hbInter, int processorCount, boolean isDebug, int a) throws IOException {
        this.handler = handler;
        this.heartbeatInterv = hbInter;
        this.isDebugging = isDebug;
        
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
                
                String currentHour = "";
                RandomAccessFile randomFile = null;
                long lastTimeFileSize = 0;
                boolean isFirst = true;
                
                
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
                    
                  //send info
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
                            LOG.info("Failed to connected " +
                                    "with " + masterAddrs[curIndex] + 
                                    "! slice: " + slice + ", type: " + type, 
                                    e);
                            // connecting failed, wait for next try
                        }
                    } // if
                    
                    

                    while (!Thread.interrupted()) {
                    	//generate info
                    	String nowHour = DateUtil.genCurrentHourStr();
                    	
                    	if (!currentHour.equals(nowHour)) {
                    		//need change file
                    		currentHour = nowHour;
                    		lastTimeFileSize = 0;
                    		//TODO:need to ensure the method of gen file name;
                    		
                    		String fileName=null;
                    		try {
                    			 fileName = logPath + currentHour + "_"+InetAddress.getLocalHost().getHostName() + logName;
                        		LOG.info("[INFO] action=check current_file="+fileName);
                        		
                    			if (randomFile != null) {
                    				randomFile.close();
                    			}
								randomFile =  new RandomAccessFile(fileName, "r");
								
							} catch (FileNotFoundException e) {
								
								LOG.error("Current-hour log file ["+fileName+"] has not created!");
								currentHour = "";
								
								try {
									Thread.sleep(3000);
									continue;
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									LOG.error("",e1);
								}
								
							} catch (IOException e) {
								LOG.error("",e);
							}
                    	}
                    	
                    	
                    	
                    	String sendMsg = "";
                    	
                    	try {
                    		if (isFirst) {
                    			lastTimeFileSize = 0;
                    			isFirst = false;
                    		}
                    		LOG.info("[INFO] action=seek filesize="+lastTimeFileSize);
							randomFile.seek(lastTimeFileSize);
							String tmp = "";
							
							int count = 0;
							while( (tmp = randomFile.readLine())!= null) {
								if(!tmp.trim().startsWith("Jan")&&!tmp.trim().startsWith("Feb")&&!tmp.trim().startsWith("Mar")&&!tmp.trim().startsWith("Apr")&&!tmp.trim().startsWith("May")&&!tmp.trim().startsWith("Jun")&&!tmp.trim().startsWith("Jul")
										&&!tmp.trim().startsWith("Aug")&&!tmp.trim().startsWith("Sep")&&!tmp.trim().startsWith("Oct")&&!tmp.trim().startsWith("Nov")&&!tmp.trim().startsWith("Dec")) {
									if(tmp.length() != 0)
										LOG.error("Bad line:" + tmp);
									continue;
								}
								count ++;
								
								if (count > 10000) {	
									if (master != null) {
			                        	if (sendMsg.length() > 0) {
			                        		hbInfo.host = sendMsg;
//			                        		master.write(sendMsg);
			                        	} else {
			                        		hbInfo.host = "";
			                        	}

			                        	master.write(hbInfo);
			                            LOG.info("FULLSEND to master " + 
			                                    masterAddrs[curIndex] + ": " + sendMsg.length() 
			                                    + "!");
			                        } else {
			                        	LOG.info("master is null !!!");
			                        }
									
									sendMsg = "";
									count = 0;
								}
								
								if (tmp.indexOf("173search") < 0) {
									continue;
								}
								
								LogMeta logmeta = MailDataParseTool.parseLog(tmp);
								
								
								if (logmeta != null) {
									sendMsg += tmp +"|~|";
								}
									
							}
							lastTimeFileSize = randomFile.length();  
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							LOG.error("",e1);
						}
                    	
                    	
                    	
                        
                        
						if (master != null) {
                        	if (sendMsg.length() > 0) {
                        		hbInfo.host = sendMsg;
//                        		master.write(sendMsg);
                        	} else {
                        		hbInfo.host = "";
                        	}

                        	master.write(hbInfo);
                            LOG.info("Heartbeat to master " + 
                                    masterAddrs[curIndex] + ": " + sendMsg.length() 
                                    + "!");
                        } else {
                        	LOG.info("master is null !!!");
                        }
                    	
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
	                            LOG.info("Failed to connected " +
	                                    "with " + masterAddrs[curIndex] + 
	                                    "! slice: " + slice + ", type: " + type, 
	                                    e);
	                            // connecting failed, wait for next try
	                        }
	                    } // if
                    	
                        
                        try {
                            long start = System.currentTimeMillis();
                            while (true) {
                                long time;
                                if (master != null)
                                    time = heartbeatInterv;
                                else
                                    time = 3000;
                                long now = System.currentTimeMillis();
                                if (now - start >= time)
                                    break;
                                Thread.sleep(Math.min(3000, 
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
                LOG.info("Failed to connected with " + 
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
            LOG.info("Message receivied. serial: " + dataSerial.serial);
        } // if
        dataSerial.data = handler.exec(dataSerial.data);
        if (isDebugging) {
            LOG.info("Sending back message. serial: " + dataSerial.serial);
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
    
	static String logPath;
	static String logName;

    public static void main(String[] args) {
    	try {
			//String[] serverHosts = args[0].split(",");
			String serverHosts[]=null;
			try {
				serverHosts = XMLUtil.readValueStrByKey("ls_ips").split(",");
			} catch (Exception e) {
				LOG.error("",e);
			}
			
			int[] serverPorts = new int[serverHosts.length];
			String strServerPort = XMLUtil.readValueStrByKey("ls_log_master_port");
			int serverPort = NumberUtils.toInt(strServerPort, 6001);
			for (int i=0; i<serverHosts.length; i++) {
				serverPorts[i] = serverPort;
			}
			
			//logPath = args[1];
			try {
				logPath=XMLUtil.readValueStrByKey("mss_logpath");
			} catch (Exception e) {
				LOG.error("",e);
			}
			//logName = args[2];
			try {
				logName=XMLUtil.readValueStrByKey("log_suffix");
			} catch (Exception e) {
				LOG.error("",e);
			}
			
			int slavePort = NumberUtils.toInt(XMLUtil.readValueStrByKey("ls_log_slave_port"), 9001);
			int heartBeatInterval = 3000;
//			if (args.length > 1){
				MinaLogSlave slave = new MinaLogSlave(null, serverHosts, serverPorts,
						"mss", slavePort,
						0, 0, 0, heartBeatInterval, 15, true);
				
			/*} else {
				MinaLogSlave slave = new MinaLogSlave(null, serverHosts, serverPorts,
						"mss", slavePort,
						0, 0, 0, heartBeatInterval, 15, false);
			}*/

		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
    }
}
