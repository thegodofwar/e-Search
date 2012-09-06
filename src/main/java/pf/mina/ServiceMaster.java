package pf.mina;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import pf.service.DataBuffer;
import pf.service.ServiceError;

/**
 * The master of the service. A master can hold a number of slaves. Each slave
 * must send heartbeat information the the master in time, otherwise that slave
 * will be considered as dead.
 * 
 * Usage:
 *   ServiceMaster master = new XxxServiceMaster(<port>);
 *   ...
 *   master.process
 *   master.processForAll
 *   master.processForSome
 *   ...
 *   master.close()
 *   
 * @author liufukun
 *
 */
public abstract class ServiceMaster {
	public static final Logger LOG=Logger.getLogger(ServiceMaster.class.getName());
    
    protected boolean isDebugging = true;
    public void setDebugging(boolean vl) {
        isDebugging = vl; 
    }
    
    /**
     * Generates an instance of SlaveInfo (or its subclass if necessary). 
     * Initializes extra fields other than those in SlaveInfo if any. The fields
     * in SlaveInfo will be initialized by the caller.
     * 
     * @param host  the host of the slave
     * @param port  the port of the slave
     * @return  the generated object instance if succ, null if failed
     */
    protected abstract SlaveInfo generateSlaveInfo(String host, int port, 
            int type);
    
    /**
     * The data-structure for holding a slave's info. The implementation could
     * override this class to add more fields.
     * 
     * Fields:
     *   host     String   the host of the slave
     *   port     int      the port of the slave
     *   type     int      the user-defined type of service
     *   timeOut  long     the time when this slave dies (this time will be 
     *                     updated when a heartbeat arrives)
     *   isDestroying      whether this slave is being destroyed. This field is
     *            boolean  set to true when the master intentionly destroy this 
     *                     slave. Some action, e.g. remove itself from slaves
     *                     should not be called when this field equals true.
     * 
     * @author David
     *
     */
    public static class SlaveInfo {
        public String host;
        public int port;
        public int type;
        public long timeOut;
        public boolean isDestroying = false;
        
        @Override
        public String toString() {
            return host + ":" + port + ", type = " + type +
                (isDestroying ? ", DESTROYING" : "");
        }
        
        public boolean equals(SlaveInfo slave) {
        	if(!this.getClass().getName().equals(slave.getClass().getName())) {
        		LOG.error("The "+this.getClass().getName()+" is not equal "+slave.getClass().getName());
        		return false;
        	}
        	if(this.host.equals(slave.host)&&this.port==slave.port&&this.type==type&&this.timeOut==timeOut&&this.isDestroying==slave.isDestroying) {
        		return true;
        	} else {
        		return false;
        	}
        }
        
    }
    /**
     * The data-structure for holding the slaves of a single slice.
     * 
     * Fields:
     *     currVersion  int      the current active version of the slaves in 
     *                           this slice, it is chosen and updated in each 
     *                           new heartbeatthe current version of the slaves 
     *                           in this slice
     *     currSlaves   TreeMap  a map of (type -> ArrayList<SlaveInfo>) 
     *                           containing the slaves of the current version
     *     otherSlaves  TreeMap  A map of (version -> ArrayList<SlaveInfo>) 
     *                           containing slaves with versions not equal to 
     *                           currVersion
     * 
     * @author David
     *
     */
    public static class SlavesInfo {
        public int currVersion;
        public TreeMap<Integer, ArrayList<SlaveInfo>> currSlaves =
            new TreeMap<Integer, ArrayList<SlaveInfo>>();
        public TreeMap<Integer, ArrayList<SlaveInfo>> otherSlaves = 
            new TreeMap<Integer, ArrayList<SlaveInfo>>();
        
        @Override
        public String toString() {
            StringBuilder str = new StringBuilder();
            str.append("currVer = ").append(currVersion);
            str.append(": ").append(currSlaves);
            if (!otherSlaves.isEmpty())
                str.append(", others: ").append(otherSlaves);
            return str.toString();
        }
    }
    /**
     * the active slaves in the master as a (slice -> SlavesInfo) map
     */
    protected TreeMap<Integer, SlavesInfo> slaves = 
        new TreeMap<Integer, SlavesInfo>();
    
    /**
     * @return  the active slaves in the master as a (slice -> SlavesInfo) map
     */
    public Map<Integer, SlavesInfo> getSlaves() {
        return slaves;
    }
    
    /**
     * Destroys the current slave. Subclass should override this method in order
     * to process further destroying action, e.g. closing data session.
     * 
     * @param slave  the slave to be destroyed
     */
    protected void destroySlave(SlaveInfo slave) {
    }
    
    /**
     * If the slave is not destroying destroy it by calling destroySlave.
     * @param slave  the slave to be destroyed
     */
    protected void doDestroySlave(SlaveInfo slave) {
        if (slave.isDestroying)
            return;
        
        slave.isDestroying = true;
        destroySlave(slave);
    }
    
    /**
     * Remove a slave in a slave-list
     * 
     * @param slaves  the slave-list
     * @param slave  the slave to be removed
     */
    protected void removeSlave(ArrayList<SlaveInfo> slaves, SlaveInfo slave) {
        Iterator<SlaveInfo> iter = slaves.iterator();
        while (iter.hasNext()) {
            SlaveInfo curSlave = iter.next();
            if (curSlave.equals(slave)) {
                iter.remove();
                doDestroySlave(slave);
                LOG.info("Slave {" + slave + "} is removed!");
            } // if
        } // while
    }
    /**
     * Remove a slae in a SlavesInfo.  Slave-lists without slaves are also
     * removed.
     * 
     * @param sliceSlaves  the SlavesInfo instance
     * @param slave  the slave to be removed
     */
    protected void removeSlave(SlavesInfo sliceSlaves, SlaveInfo slave) {
        Iterator<ArrayList<SlaveInfo>> iter;
        if (sliceSlaves.currSlaves.size() > 0) {
            iter = sliceSlaves.currSlaves.values().iterator();
            while (iter.hasNext()) {
                ArrayList<SlaveInfo> slaves = iter.next();
                removeSlave(slaves, slave);
                if (slaves.size() == 0) {
                    iter.remove();
                } // if
            } // while
        } // if
        
        if (sliceSlaves.otherSlaves.size() > 0) {
            iter = sliceSlaves.otherSlaves.values().iterator();
            while (iter.hasNext()) {
                ArrayList<SlaveInfo> slaves = iter.next();
                removeSlave(slaves, slave);
                if (slaves.size() == 0)
                    iter.remove();
            } // while
        } // if
    }
    /**
     * Remove a slave from the master
     * 
     * @param slave  the slave to be removed
     */
    protected void removeSlave(SlaveInfo slave) {
        synchronized (slaves) {
            for (SlavesInfo sliceSlaves: slaves.values()) {
                removeSlave(sliceSlaves, slave);
            } // for slavesInfo
        }
    }
    
    /**
     * Find the timeout slaves and remove them from the slave-list.
     * 
     * @param slaves  the slave-list
     * @param now  the current time
     */
    void checkTimeout(ArrayList<SlaveInfo> slaves, long now) {
        Iterator<SlaveInfo> iter = slaves.iterator();
        while (iter.hasNext()) {
            SlaveInfo slave = iter.next();
            if (now >= slave.timeOut) {
                iter.remove();
                doDestroySlave(slave);
                
                LOG.info("Slave {" + slave + "} timeout!");
            } // if
        } // while
    }
    /**
     * Find the timeout slaves and remove them from the SlavesInfo.
     * 
     * @param sliceSlaves  the SlavesInfo instance
     * @param now  the current time
     */
    void checkTimeout(SlavesInfo sliceSlaves, long now) {
        Iterator<ArrayList<SlaveInfo>> iter;
        // check currSlaves
        if (sliceSlaves.currSlaves.size() > 0) {
            iter = sliceSlaves.currSlaves.values().iterator();
            while (iter.hasNext()) {
                ArrayList<SlaveInfo> slaves = iter.next();
                checkTimeout(slaves, now);
                if (slaves.size() == 0)
                    iter.remove();
            } // while
        } // if
        // check otherSlaves
        if (sliceSlaves.otherSlaves.size() > 0) {
            iter = sliceSlaves.otherSlaves.values().iterator();
            while (iter.hasNext()) {
                ArrayList<SlaveInfo> slaves = iter.next();
                checkTimeout(slaves, now);
                if (slaves.size() == 0)
                    iter.remove();
            } // while
        } // if
    }
    /**
     * Choose the current version in a SlavesInfo.
     * The current choosing policy will make sure that the chosen version 
     *  1) contains the most slaves (whatever type)
     *  2) is of higher version if same number of slaves for another version 
     *     was found
     *     
     * If a new version is chosen, related adjustment is done.
     * 
     * @param slice  the slice number
     * @param sliceSlaves  the SlavesInfo instance
     */
    void chooseCurrentVersion(int slice, SlavesInfo sliceSlaves) {
        int curCount = 0;
        if (sliceSlaves.currSlaves.size() > 0) {
            for (ArrayList<SlaveInfo> slaves: sliceSlaves.currSlaves.values())
                curCount += slaves.size();
        } // if
        
        if (sliceSlaves.otherSlaves.size() > 0) {
            int otherMaxCount = -1;
            Integer otherMaxVer = 0;
            for (Map.Entry<Integer, ArrayList<SlaveInfo>> ent: 
                    sliceSlaves.otherSlaves.entrySet()) {
                if (ent.getValue().size() > otherMaxCount ||
                        ent.getValue().size() == otherMaxCount &&
                        ent.getKey() > otherMaxVer) {
                    otherMaxVer = ent.getKey();
                    otherMaxCount = ent.getValue().size();
                } // if
            } // for ent
            
            if (otherMaxCount > curCount ||
                    otherMaxCount == curCount && 
                    otherMaxVer > sliceSlaves.currVersion) {
                LOG.info("Version of slice " + slice + " is updated from " 
                        + sliceSlaves.currVersion + " to " + otherMaxVer);
                /*
                 * If otherMaxVer has more slaves or the same number of slaves 
                 * and with a higher vresion, update the current version to 
                 * otherMaxVer
                 */
                if (curCount > 0) {
                    /*
                     * merge current version and put it into otherSlaves
                     */
                    ArrayList<SlaveInfo> verSlaves = new ArrayList<SlaveInfo>();
                    for (ArrayList<SlaveInfo> slaves: 
                            sliceSlaves.currSlaves.values()) {
                        verSlaves.addAll(slaves);
                    } // for slaves
                    sliceSlaves.otherSlaves.put(sliceSlaves.currVersion, 
                            verSlaves);
                } // if
                /*
                 * Put slaves of otherMaxVer into currSlaves
                 */
                // set current version
                sliceSlaves.currVersion = otherMaxVer;
                // extract slaves out from otherSlaves
                ArrayList<SlaveInfo> verSlaves = sliceSlaves.otherSlaves.get(
                        otherMaxVer);
                sliceSlaves.otherSlaves.remove(otherMaxVer);
                // put slaves into curSlaves
                sliceSlaves.currSlaves.clear();
                for (SlaveInfo slave: verSlaves) {
                    ArrayList<SlaveInfo> typeSlaves = 
                        sliceSlaves.currSlaves.get(slave.type);;
                    if (typeSlaves == null) {
                        // create a new list if not found
                        typeSlaves = new ArrayList<SlaveInfo>(2);
                        sliceSlaves.currSlaves.put(slave.type, typeSlaves);
                    } // if
                    typeSlaves.add(slave);
                } // for slave
            } // iof
        } // if
    }
    
    /**
     * Generate a new instance of SlaveInfo instance
     */
    private SlaveInfo newSlaveInfo(String host, int port, int type, long now,
            int ttl) {
        SlaveInfo slave = generateSlaveInfo(host,  port, type);
        if (slave == null)
            return null;
        slave.host = host;  slave.port = port;  slave.type = type;
        slave.timeOut = now + ttl;
        return slave;
    }
    /**
     * Generates a new SlaveInfo list initialized with one specified slave
     */
    private ArrayList<SlaveInfo> newSlaveInfoList(String host, int port,
            int type, long now, int ttl) {
        SlaveInfo slave = newSlaveInfo(host, port, type, now, ttl);
        if (slave == null)
            return null;
        ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>(2);
        slaveList.add(slave);
        return slaveList;
    }
    /**
     * Find a specified slave in a slave-list
     */
    private SlaveInfo findSlave(ArrayList<SlaveInfo> slaves, String host, 
            int port, int type) {
        for (int i = 0; i < slaves.size(); i ++) {
            SlaveInfo slave = slaves.get(i);
            if (slave.type == type && slave.port == port &&
                    slave.host.equals(host))
                return slave;
        } // for i
        return null;
    }
    /**
     * Processes a heartbeat from a slave.
     * 
     * @param host  the host of the slave
     * @param port  the port of the slave service
     * @param type  the type of the slave
     * @param slice  the slice of the slave
     * @param version  the version of the slave
     * @param ttl  the time-to-live of the slave. After ttl (msec), the slave
     *             dies if no heartbeats received. If ttl is less than or equal 
     *             to zero, this slave will be removed from the master.
     * @return  true if the slave is successfully added, false otherwise.
     */
    public boolean heartbeat(String host, int port, int type, int slice, 
            int version, int ttl) {
        synchronized (slaves) {
            SlavesInfo sliceSlaves = slaves.get(slice);
            long now = System.currentTimeMillis();
            if (sliceSlaves == null) {
                // a new slave with a new slice
                LOG.info("A slave (" + host + ":" + port + ") (type = " + type 
                        + ") with new slice " + slice + " and version " 
                        + version + " is found.");
                sliceSlaves = new SlavesInfo();
                slaves.put(slice, sliceSlaves);
                
                sliceSlaves.currVersion = version;
                
                if (ttl <= 0)
                    return false;
                ArrayList<SlaveInfo> slaveList = newSlaveInfoList(
                        host, port, type, now, ttl);
                if (slaveList == null)
                    return false;
                sliceSlaves.currSlaves.put(type, slaveList);
                return true;
            } // if
            checkTimeout(sliceSlaves, now);
            
            if (sliceSlaves.currVersion == version) {
                /*
                 * the heartbeat is from a slave of the ccurrent version
                 * related data were store at sliceSlaves.currSlaves
                 */
                ArrayList<SlaveInfo> typeSlaves = 
                    sliceSlaves.currSlaves.get(type);
                if (typeSlaves == null) {
                    // the first hearbeat of this type
                    LOG.info("First heartbeat of a new type (" + type + ") " +
                            "slave (" + host + ":" + port + ", slice = " + slice 
                            + ", current version = " + version + ")");
                    if (ttl <= 0)
                        return false;
                    ArrayList<SlaveInfo> slaveList = newSlaveInfoList(
                            host, port, type, now, ttl);
                    if (slaveList == null)
                        return false;
                    sliceSlaves.currSlaves.put(type, slaveList);
                    chooseCurrentVersion(slice, sliceSlaves);
                    return true;
                }

                SlaveInfo slave = findSlave(typeSlaves, host, port, type);
                if (slave == null) {
                    // a new slave
                    LOG.info("First heartbeat of a slave(" + host + ":" + port 
                            + ", slice = " + slice + ", type = " + type 
                            + ", current version = " + version + ")");
                    if (ttl <= 0)
                        return false;
                    slave = newSlaveInfo(host, port, type, now, ttl);
                    if (slave == null)
                        return false;
                    typeSlaves.add(slave);
                    chooseCurrentVersion(slice, sliceSlaves);
                    return true;
                } // if
                // update the slave
                LOG.info("Heartbeat of a slave(" + host + ":" + port 
                        + ", slice = " + slice + ", type = " + type 
                        + ", current version = " + version + ")"); 
                slave.timeOut = now + ttl;
                chooseCurrentVersion(slice, sliceSlaves);
                return true;
            } // if
            /*
             * the heartbeat is from a slave of not-current version
             * related data were stored at sliceSlaves.otherSlaves 
             */
            ArrayList<SlaveInfo> verSlaves = 
                sliceSlaves.otherSlaves.get(version);
            
            if (verSlaves == null) {
                /*
                 * data of this version is not found
                 */
                LOG.info("First heartbeat of a new non-active version (" 
                        + version + ") slave(" + host + ":" + port 
                        + ", slice = " + slice + ", type = " + type 
                        + ", current version = " + sliceSlaves.currVersion 
                        + ")"); 
                ArrayList<SlaveInfo> slaveList = newSlaveInfoList(host, port, 
                        type, now, ttl);
                if (slaveList == null)
                    return false;
                sliceSlaves.otherSlaves.put(version, slaveList);
                chooseCurrentVersion(slice, sliceSlaves);
                return true;
            } // if
            
            SlaveInfo slave = findSlave(verSlaves, host, port, type);
            if (slave == null) {
                // a new slave
                LOG.info("First heartbeat of a non-active slave(" + host + ":" 
                        + port + ", slice = " + slice + ", type = " + type 
                        + ", heartbeat version = " + version + 
                        ", current version: " + sliceSlaves.currVersion + 
                        ")"); 
                slave = newSlaveInfo(host, port, type, now, ttl);
                if (slave == null)
                    return false;
                verSlaves.add(slave);
                chooseCurrentVersion(slice, sliceSlaves);
                return true;
            } // if
            
            LOG.info("Heartbeat of a non-active slave(" + host + ":" 
                    + port + ", slice = " + slice + ", type = " + type 
                    + ", heartbeat version = " + version + 
                    ", current version: " + sliceSlaves.currVersion + 
                    ")");
            if (ttl <= 0)
                removeSlave(slave);
            else
                slave.timeOut = now + ttl;
            chooseCurrentVersion(slice, sliceSlaves);
            return true;
        }
    }
    
    public static Random rand = new Random();
    /**
     * Choose a slave from a slaves list
     * 
     * @param slaves  the slave list
     * @return  the chosen one, or null if no slave available
     */
    SlaveInfo chooseSlave(ArrayList<SlaveInfo> slaves) {
        if (slaves == null)
            return null;
        if (slaves.size() == 0)
            return null;
        if (slaves.size() == 1)
            return slaves.get(0);
        return slaves.get(rand.nextInt(slaves.size()));
    }
    
    /**
     * Choose a slave for a spicified slice.
     * If the chosen one is timeout, it will be removed and a new one will be
     * chosen.
     * 
     * @param slice  the slice number
     * @return  the chosen slave, or null if no available slave found
     */
    protected SlaveInfo chooseSlave(int slice, int type) {
        SlavesInfo sliceSlaves = slaves.get(slice);
        if (sliceSlaves == null)
            return null;
        synchronized (sliceSlaves) {
            ArrayList<SlaveInfo> typeSlaves = sliceSlaves.currSlaves.get(type);
            SlaveInfo slave = chooseSlave(typeSlaves);
            if (slave == null)
                return null;
            long now = System.currentTimeMillis();
            if (now >= slave.timeOut) {
                checkTimeout(typeSlaves, now);
                slave = chooseSlave(typeSlaves);
            } // if
            
            return slave;
        }
    }
    
    /**
     * Choose a slave for each slice
     * 
     * @return  the array of the slaves with one slave for each slice. Some
     *          element of this array could be null.
     */
    protected SlaveInfo[] chooseSlaves(int type) {
        synchronized (slaves) {
            SlaveInfo[] selected = new SlaveInfo[slaves.size()];
            int idx = 0;
            for (Integer slice: slaves.keySet()) {
                selected[idx ++] = chooseSlave(slice, type);
            } // for sliceSlaves
            return selected;
        }
    }
    /**
     * Chooses a slave for each of the spicified slices.
     * 
     * @param slices  the array of the slice-numbers
     * @return  the array of the selected slaves. Some element of this array 
     *          could be null if no slaves for that slice were found
     */
    protected SlaveInfo[] chooseSlaves(int[] slices, int type) {
        SlaveInfo[] selected = new SlaveInfo[slices.length];
        for (int i = 0; i < slices.length; i ++) {
            selected[i] = chooseSlave(slices[i], type);
        } // for i
        return selected;
    }
    
    /**
     * Send the data to a slave with specified slice-number 
     * 
     * @param slice  the slice-number
     * @param type  the type of the service
     * @param in  the input data
     * @param ttl  the time-to-live in milli-seconds
     * @param error  the ServiceError instance containing the error status
     *               and reason
     * @return  the returned output data
     * @throws InterruptedException  if the current thread is interrupted 
     */
    public DataBuffer process(int slice, int type, DataBuffer in, int ttl,
            ServiceError error) throws InterruptedException {
        EntryFuture res = asyncProcess(slice, type, in, ttl, error);
        if (res == null)
            return null;
        return res.get();
    }
    
    public class EntryFuture {
        ProcessEntry entry;
        ServiceError error;
        public EntryFuture(ProcessEntry entry, ServiceError error) {
            this.entry = entry;
            this.error = error;
        }
        
        private static final int WAITING = 0;
        private static final int FAILED = 1;
        private static final int SUCC = 2;
        private int status = WAITING;
        public DataBuffer get() throws InterruptedException {
            if (status == WAITING) {
                int ttl = (int)(entry.timeout - System.currentTimeMillis());
                if (ttl < 0) {
                    status = FAILED;
                    clearProcessEntry(entry);
                } else {
                    if (!entry.semaphore.tryAcquire(1, ttl, 
                            TimeUnit.MILLISECONDS)) {
                        status = FAILED;
                        clearProcessEntry(entry);
                    } else {
                        status = SUCC;
                    } // else
                } // else
            } // if
            
            if (status == SUCC) {
                error.setStatus(ServiceError.STATUS_SUCC);
                return entry.resArr[0];
            } else {
                error.setStatus(ServiceError.STATUS_FAILED);
                error.setReason(ServiceError.REASON_TIMEOUT);
                return null;
            } // else
        }

        public boolean isDone() {
            if (status != WAITING)
                return true;
            if (System.currentTimeMillis() > entry.timeout) {
                status = FAILED;
                clearProcessEntry(entry);
            } // if
            
            if (!entry.semaphore.tryAcquire(entry.resArr.length))
                return false;
            error.setStatus(ServiceError.STATUS_SUCC);
            status = SUCC;
            return true;
        }
    }
    
    /**
     * Sends the data to a slave with specified slice-number in an asychronized
     * way. 
     * 
     * @param slice  the slice-number
     * @param type  the type of the service
     * @param in  the input data
     * @param ttl  the time-to-live in milli-seconds
     * @param error  the ServiceError instance containing the error status
     *               and reason
     * @return  a future instance for wait and get the returned output data. 
     *          null if failed before sending the request (e.g. no slave)
     */
    public EntryFuture asyncProcess(int slice, int type, DataBuffer in, 
            final int ttl, final ServiceError error) {
        if (isDebugging) {
            LOG.info("asyncProcess(" + slice + ", " + type + ", in, " +
                    ttl + ", error");
        } // if
        SlaveInfo slave = chooseSlave(slice, type);
        if (slave == null) {
            error.setStatus(ServiceError.STATUS_FAILED);
            error.setReason(ServiceError.REASON_NOSLAVE);
            return null;
        } // if
        
        final ProcessEntry entry = generateProcessEntry(
                System.currentTimeMillis() + ttl);
        entry.resArr = new DataBuffer[1];
        entry.in = in;
        entry.index = 0;
        entry.semaphore = new Semaphore(0);
        entry.slave = slave;
        
        internalAsyncProcess(entry);
        return new EntryFuture(entry, error);
    }
    /**
     * The data-structure for an execution entry. One entry represents one
     * time of calling to the slave. The results should be place on the correct
     * place in the results array.
     * 
     * @author David
     *
     */
    public static class ProcessEntry {
        /**
         * The information of the slave for this execution
         */
        public SlaveInfo slave;
        /**
         * The array of the list
         */
        public DataBuffer[] resArr;
        /**
         * The index of this entry in the resArr.
         */
        public int index;
        /**
         * The input data
         */
        public DataBuffer in;
        /**
         * The samaphore. After the slave responses, sam.release() semaphore 
         * should be called
         */
        public Semaphore semaphore;
        /**
         * the time when this execution is out of date.
         */
        public long timeout;
    }
    
    /**
     * Process for deleting index
     * @author DevUser
     *
     */
    public static class DeleteProcessEntry {
        /**
         * The information of the slave for this execution
         */
        public SlaveInfo slave;
        /**
         * The array of the list
         */
        public DataBuffer[] delResult;
        /**
         * The index of this entry in the resArr.
         */
        public int index;
        /**
         * The input data
         */
        public DataBuffer in;
        /**
         * The samaphore. After the slave responses, sam.release() semaphore 
         * should be called
         */
        public Semaphore semaphore;
        /**
         * the time when this execution is out of date.
         */
        public long timeout;
    }
    
    /**
     * Generate a new ProcessEntry (or its derived class). Initialized the 
     * new fields other than those in ProcessEntry if any.
     * 
     * @param  timeout  the time when the execution is out of date
     * @return The instance of a ProcessEntry
     */
    protected ProcessEntry generateProcessEntry(long timeout) {
        ProcessEntry entry = new ProcessEntry();
        entry.timeout = timeout;
        return entry;
    }
    
    /**
     * generate ProcessEntry for deleting index
     * @return
     */
    protected DeleteProcessEntry generateDeleteProcessEntry(long timeout) {
    	DeleteProcessEntry deleteEntry = new DeleteProcessEntry();
    	deleteEntry.timeout = timeout;
    	return deleteEntry;
    }
    
    /**
     * This function is called when this entry is no-longer used. The default 
     * implementation does nothing. Subclass can override this method to remove
     * this entry from the entry list if any.
     * 
     * NOTE: for the convenience of implementation, the entry may have been 
     * removed out before calling this method. 
     * 
     * @param entry  the entry to be cleared
     */
    protected void clearProcessEntry(ProcessEntry entry) {
    }
    
    /**
     * clear delete process entry
     * @param deleteEntry
     */
    protected void clearDeleteProcessEntry(DeleteProcessEntry deleteEntry) {
    	
    }
    
    /**
     * Sends the input to all of the slices
     * 
     * @param in  the input data
     * @param type  the type of the service
     * @param ttl  the time-to-live in miliseconds.
     * @return  the array of the returned results.
     * @param error  the ServiceError instance containing the error status
     *               and reason. 
     *               Possible status/reasons include:
     *                 1) STATUS_SUCC
     *                   The results are completely obtained
     *                 2) STATUS_PARTIAL/REASON_TIMEOUT
     *                   At least one slave times out
     * @throws InterruptedException  if the current thread is interrupted during
                   waiting for the results
     */
    public DataBuffer[] processForAll(int type, DataBuffer in, int ttl, 
            ServiceError error) throws InterruptedException {
        EntriesFuture res = asyncProcessForAll(type, in, ttl, error);
        if (res == null)
            return null;
        return res.get();
    }
    
    public int[] deleteProcessForAll(int type,DataBuffer delIn,int ttl) {
    	return deleteAsyncProcessForAll(type,delIn,ttl);
    }
    
    public class EntriesFuture {
        ProcessEntry[] entries;
        long timeout;
        Semaphore semaphore;
        DataBuffer[] resArr;
        boolean slaveNotFound;
        
        ServiceError error;
        public EntriesFuture(ProcessEntry[] entries, long timeout, 
                ServiceError error, Semaphore sam, DataBuffer[] resArr) {
            this.entries = entries;
            this.timeout = timeout;
            this.error = error;
            this.semaphore = sam;
            this.resArr = resArr;
            
            for (int i = 0; i < entries.length; i ++)
                if (entries[i] == null)
                    slaveNotFound = true;
        }
        
        private static final int WAITING = 0;
        private static final int FAILED = 1;
        private static final int SUCC = 2;
        private int status = WAITING;
        
        private void clearProcessEntries() {
            for (int i = 0; i < entries.length; i ++) {
                if (entries[i] != null && resArr[i] == null)
                    clearProcessEntry(entries[i]);
            } // for i
        }

        public DataBuffer[] get() 
                throws InterruptedException {
            if (status == WAITING) {
                int ttl = (int)(this.timeout - System.currentTimeMillis());
                if (ttl < 0) {
                    status = FAILED;
                    clearProcessEntries();
                } else {
                    if (!semaphore.tryAcquire(entries.length, ttl, 
                            TimeUnit.MILLISECONDS)) {
                        status = FAILED;
                        clearProcessEntries();
                    } else {
                        status = SUCC;
                    } // else
                } // else
            } // if
            
            if (status == SUCC) {
                if (slaveNotFound) {
                    error.setStatus(ServiceError.STATUS_PARTIAL);
                    error.setReason(ServiceError.REASON_NOSLAVE);
                } else {
                    error.setStatus(ServiceError.STATUS_SUCC);
                } // else
            } else {
                error.setStatus(ServiceError.STATUS_PARTIAL);
                error.setReason(ServiceError.REASON_TIMEOUT);
            } // else
            return resArr;
        }

        public boolean isDone() {
            if (status != WAITING)
                return true;
            if (System.currentTimeMillis() > this.timeout) {
                status = FAILED;
                clearProcessEntries();
            } // if
            
            if (!semaphore.tryAcquire(entries.length))
                return false;
            error.setStatus(ServiceError.STATUS_SUCC);
            status = SUCC;
            return true;
        }
    }
    
    /**
     * Sends the input to all of the slices in an asynchronized way.
     * 
     * @param in  the input data
     * @param type  the type of the service
     * @param ttl  the time-to-live in miliseconds.
     * @param error  the ServiceError instance containing the error status
     *               and reason. 
     *               Possible status/reasons include:
     *                 1) STATUS_SUCC
     *                   The results are completely obtained
     *                 2) STATUS_PARTIAL/REASON_TIMEOUT
     *                   At least one slave times out
     *                 2) STATUS_PARTIAL/REASON_NOSLAVE
     *                   Some slices do not find correponding slaves.
     * @return  the EntriesFuture for waiting and getting the array of the 
     *          returned results.
     */
    public EntriesFuture asyncProcessForAll(int type, DataBuffer in, int ttl, 
            ServiceError error) {
    	SlaveInfo[] selectedSlaves = chooseSlaves(type);
        DataBuffer[] res = new DataBuffer[selectedSlaves.length];
        ProcessEntry[] entries = new ProcessEntry[selectedSlaves.length];
        Semaphore sam = new Semaphore(0);
        long timeout = System.currentTimeMillis() + ttl;
        // asynchronized execute the entry
        for (int i = 0; i < res.length; i ++) {
            if (selectedSlaves[i] == null) {
                // no alive slave, ignore this
                sam.release();
            } else {
                ProcessEntry processEntry = generateProcessEntry(timeout);
                processEntry.index = i;
                processEntry.slave = selectedSlaves[i];
                processEntry.in = in;
                processEntry.resArr = res;
                processEntry.semaphore = sam;
                entries[i] = processEntry;
                internalAsyncProcess(processEntry);
            } // else
        } // for i
        
        return new EntriesFuture(entries, timeout, error, sam, res);
    }
    
    public int[] deleteAsyncProcessForAll(int type,DataBuffer delIn,int ttl) {
    	SlaveInfo[] selectedSlaves = chooseSlaves(type);
    	DataBuffer[] delResult = new DataBuffer[selectedSlaves.length];
    	DeleteProcessEntry[] entries = new DeleteProcessEntry[selectedSlaves.length];
    	Semaphore sam = new Semaphore(0);
    	long timeout = System.currentTimeMillis() + ttl;
    	// asynchronized execute the entry
    	 for (int i = 0; i < delResult.length; i ++) {
             if (selectedSlaves[i] == null) {
                 // no alive slave, ignore this
                 sam.release();
             } else {
                 DeleteProcessEntry deleteProcessEntry = generateDeleteProcessEntry(timeout);
                 deleteProcessEntry.index = i;
                 deleteProcessEntry.slave = selectedSlaves[i];
                 deleteProcessEntry.in = delIn;
                 deleteProcessEntry.delResult = delResult;
                 deleteProcessEntry.semaphore = sam;
                 entries[i] = deleteProcessEntry;
                 deleteInternalAsyncProcess(deleteProcessEntry);
             } // else
         } // for i
    	 
    	 boolean slaveNotFound =false;
    	 boolean allSuccess = false;
    	 
    	 for (int i = 0; i < entries.length; i ++) {
             if (entries[i] == null) {
                 slaveNotFound = true;
             }
    	 }
    	 
    	 int leftTime = (int)(timeout - System.currentTimeMillis());
    	 
    	 if(leftTime < 0) {
    		 for (int i = 0; i < entries.length; i ++) {
                 if (entries[i] != null && delResult[i] == null) {
                     clearDeleteProcessEntry(entries[i]);
                 } else if(entries[i] != null && delResult[i] != null) {
                	 LOG.info("Delete Timeout But Success At Same Time!");
                 }
             } // for i 
    	 } else {
    		 try {
				if (!sam.tryAcquire(entries.length, leftTime, 
				         TimeUnit.MILLISECONDS)) {
					 for (int i = 0; i < entries.length; i ++) {
				         if (entries[i] != null && delResult[i] == null) {
				             clearDeleteProcessEntry(entries[i]);
				         } else if(entries[i] != null && delResult[i] != null) {
				        	 LOG.info("Delete Not Timeout And Acquire Semaphore Failure But Success At Same Time!");
				         }
				     } // for i 
				 } else {
				     allSuccess = true;
				 }
			} catch (InterruptedException e) {
				LOG.error("",e);
			} // else
    	 }
    	 
    	 if(allSuccess==true) {
    		 if(slaveNotFound==true) {
    			 LOG.error("Some Slices' Slave Not Found!");
    		 } else {
    			 LOG.info("Delete All Slices' Indexes Successfully!");
    		 }
    	 } else {
    		 LOG.error("Delete Slices' Indexes Faliure!");
    	 }
    	 
    	 LOG.info("selectedSlaves.length = "+selectedSlaves.length+" delResult.length = "+delResult.length);
    	
    	 int delCode[] = new int[delResult.length];
    	 for(int j=0;j<delResult.length;j++) {
    		 try {
				delCode[j] = delResult[j].readVInt();
			} catch (IOException e) {
				LOG.error("",e);
			}
    	 }
    	 return delCode;
    }
    
    /**
     * Execute an entry in an asynchronized style.
     * 
     * @param entry  the ExecEntry instance
     * @throws InterruptedException  if the current thread is interrupred
     */
    protected abstract void internalAsyncProcess(ProcessEntry entry);
    
    /**
     * delete index according to mskey
     * @param entry
     */
    protected abstract void deleteInternalAsyncProcess(DeleteProcessEntry entry);
    
    /**
     * Process for some data. Each data is sent to the slice in slices array,
     * then results are collected.
     * 
     * @param type  the type of the service
     * @param dataArray  the array of input data
     * @param slices  the slices of each input data. The length of slices MUST
     *                be the same as dataArray
     * @param ttl  the time-to-live in milli-seconds
     * @param error  the ServiceError instance containing the error status
     *               and reason
     * @return  the array of output processed results. The length of the 
     *          returned array is the same as dataArray, but some of the 
     *          elements maybe null if the correponding processing times out.
     * @throws InterruptedException  if the current thread was interrupted
     */
    public DataBuffer[] processForSome(int type, DataBuffer[] dataArray, 
            int[] slices, int ttl, ServiceError error) 
            throws InterruptedException {
        EntriesFuture res = asyncProcessForSome(type, dataArray, slices, ttl, 
                error);
        if (res == null)
            return null;
        return res.get();
    }
    
    /**
     * Processes for some data in an asynchronized way. Each data is sent to the 
     * slice in slices array, then results are collected.
     * 
     * @param type  the type of the service
     * @param dataArray  the array of input data
     * @param slices  the slices of each input data. The length of slices MUST
     *                be the same as dataArray
     * @param ttl  the time-to-live in milli-seconds
     * @param error  the ServiceError instance containing the error status
     *               and reason
     * @return  the EntriesFuture for waiting and getting the array of output 
     *          processed results. The length of the returned array is the same 
     *          as dataArray, but some of the elements maybe null if the 
     *          correponding processing times out.
     */
    public EntriesFuture asyncProcessForSome(int type, DataBuffer[] dataArray, 
            int[] slices, int ttl, ServiceError error) {
        SlaveInfo[] selectedSlaves = chooseSlaves(slices, type);
        DataBuffer[] res = new DataBuffer[slices.length];
        ProcessEntry[] entries = new ProcessEntry[slices.length];
        Semaphore sam = new Semaphore(0);
        long timeout = System.currentTimeMillis() + ttl;
        // asynchronized execute the entry
        for (int i = 0; i < res.length; i ++) {
            if (selectedSlaves[i] == null) {
                // no alive slave, ignore this
                sam.release();
            } else {
                ProcessEntry processEntry = generateProcessEntry(timeout);
                processEntry.index = i;
                processEntry.slave = selectedSlaves[i];
                processEntry.in = dataArray[i];
                processEntry.resArr = res;
                processEntry.semaphore = sam;
                entries[i] = processEntry;
                internalAsyncProcess(processEntry);
            } // else
        } // for i
        
        return new EntriesFuture(entries, timeout, error, sam, res);
    }
}

