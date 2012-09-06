package wanmei.mina;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import odis.serialize.lib.StringWritable;
import outpost.search.Hits;
import outpost.search.ParameterWritable;
import outpost.search.Hits.Hit;
import outpost.service.DataBuffer;
import outpost.service.ServiceError;
import wanmei.mina.ServiceMaster;
import wanmei.mina.ServiceMaster.EntriesFuture;
import wanmei.mina.handler.DeleteIHandler;
import wanmei.mina.handler.IHandler;
import wanmei.mina.handler.LocalIndexSearcher;
import toolbox.misc.collection.IntPriorityQueue;
import toolbox.misc.collection.IntPriorityQueue.IntComparator;

/**
 * The adapter of the local-index handler.
 * 
 * Two classes are defined: IndexSearcher and IndexClientSearcher.
 * The distributed searcher will send a search request from an 
 * IndexClientSearcher, and every LocalIndexServer can receive this request and
 * returns its results to the IndexClientSearcher. Results (hits) are then
 * merged and returned.
 * 
 * The data sent from IndexSearcher to LocalIndexServer is in the 
 * following format:
 *   <query(StringWritable)> 
 *   <len(vint)> 
 *   <params(ParameterWritable)>
 *   
 * The data return from LocalIndexServer to IndexSearcher is in the 
 * following format:
 *   <code(vint)>
 *   <user-data>
 *   <total(vint)>
 *   <count(vint)>
 *   <hit(Hit)> ... <hit(Hit)>
 * 
 * @author David
 *
 */
public abstract class IndexSearchHandlerAdapters {
	public static final Logger LOG=Logger.getLogger(IndexSearchHandlerAdapters.class.getName()); 
    
    /**
     * the code indicating a failed operation
     */
    public static int CODE_FAILED = 0;
    /**
     * the code indicating a successful operation
     */
    public static int CODE_SUCC   = 1;

    /**
     * A static DataBuffer representing a failure
     */
    public static final DataBuffer DB_FAILED = new DataBuffer();
    static {
        try {
            IndexSearchHandlerAdapters.DB_FAILED.writeVInt(CODE_FAILED);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * An class deliverying a local-index-searching service. After creating the
     * object, the searcher need to be set by calling setSearcher().
     * 
     * @author David
     *
     */
    public static class LocalIndexServer implements IHandler {
        protected LocalIndexSearcher searcher;
        
        public DataBuffer exec(DataBuffer in) {
            StringWritable query = new StringWritable();
            try {
                /*
                 * Read from master
                 */
                query.readFields(in);
                int start = 0;
                int len = in.readVInt();
                HashMap<String, String[]> params = new HashMap<String, String[]>();
                ParameterWritable.readParameters(in, params);
                /*
                 * search
                 */
                Hits hits = searcher.search(query.get(), start, len, params);
                /*
                 * Return to master
                 */
                DataBuffer res = new DataBuffer();
                res.writeVInt(CODE_SUCC);
                hits.writeUserData(res);
                res.writeVInt(hits.getTotal());
                res.writeVInt(hits.size());
                for (Hit hit: hits)
                    hit.writeFields(res);
                
                return res;
            } catch (Exception e) {
                LOG.error("",e);
                return DB_FAILED;
            }
        }

        /**
         * @return the searcher instance.
         */
        public LocalIndexSearcher getSearcher() {
            return searcher;
        }
        /**
         * Set the searher instance.
         * 
         * @param searcher  the LocalIndexSearcher instance for searching
         */
        public void setSearcher(LocalIndexSearcher searcher) {
            this.searcher = searcher;
        }
    }
    
    /**
     * processing delete local handler
     * @author liufukun
     *
     */
    public static class LocalIndexDelete implements DeleteIHandler {
    	protected LocalIndexSearcher searcher;
    	
		@Override
		public DataBuffer exec(DataBuffer delIn) {
			try {
				HashMap<String,String[]> params = new HashMap<String,String[]>();
				ParameterWritable.readParameters(delIn, params);
				/**
				 * delete
				 */
				int delResult = searcher.delete(params);
				/**
				 * Return to master
				 */
				DataBuffer delRes = new DataBuffer();
				delRes.writeVInt(delResult);
				return delRes;
			} catch (Exception e) {
				LOG.error("",e);
				return DB_FAILED;
			}
		}
    	
		/**
         * @return the searcher instance.
         */
		public LocalIndexSearcher getSearcher() {
			return searcher;
		}
		/**
         * Set the searher instance.
         * 
         * @param searcher  the LocalIndexSearcher instance for searching
         */
		public void setSearcher(LocalIndexSearcher searcher) {
			this.searcher = searcher;
		}
    }
    
    /**
     * The class that connects with several local-index-servers through a
     * Service-Master/Slave structure.
     * 
     * Fields:
     *   master       ServiceMaster  the master instance
     *   serviceType  int            the type of this service
     * @author David
     *
     */
    public static class IndexSearcher {
        protected ServiceMaster master;
        protected int serviceType;

        public ServiceMaster getMaster() {
            return master;
        }
        public void setMaster(ServiceMaster master) {
            this.master = master;
        }
        
        public int getServiceType() {
            return serviceType;
        }
        public void setServiceType(int serviceType) {
            this.serviceType = serviceType;
        }
        
        /**
         * Returns a new instance of Hit(). The returned instance can be a sub-
         * class instance but it must be corresponding to the local-server part
         * on data serialization, i.e. its readFields should be compatible with
         * writeFields in local-server part.
         * 
         * The default implementation returns a new instance of Hits.Hit.
         * 
         * The sub-class can also override this method to allocate the object
         * from a object pool. Each instance is guarenteed not to be used after
         * returned from search() (i.e. can be collected back again) 
         * 
         * @return  the new Hit instance. This object may be returned as an 
         *          element of search().
         */
        protected Hit newHit() {
            return new Hit();
        }
        /**
         * Returns a new instance of Hits(). The returned instance can be a sub-
         * class instance but it must be corresponding to the local-server part
         * on data serialization, i.e. its clearUserData/appendUserData should
         * be compatible with writeUserData in local-server part.
         * 
         * The default implementation returns a new instance of Hits.
         * 
         * The sub-class can also override this method to allocate the object
         * from a object pool. Each instance is guarrenteed not to be used
         * after returned from search() (i.e. can be collected back again)
         * 
         * @return  the new Hits instance. This object will be returned as the
         *          result of search()
         */
        protected Hits newHits() {
            return new Hits();
        }
        /**
         * Merge hits in out, extract the <start-> len> parts in the merged
         * sorted list.
         * 
         * The default implementation assumes that input lists are sorted hits,
         * and the combination is performed using a priority queue.
         *  
         * @param outs  the list of DataBuffer output
         * @param start  the start position of the results
         * @param len  the maximum number of results returned
         * @param params  the params passed to search() 
         * @return  the extracted hits as a Hits instance
         * @throws IOException if an I/O error occurs
         */
        protected Hits mergeOut(DataBuffer[] outs, int start, int len, 
                Map<String, String[]> params) throws IOException {
            int[] remains = new int[outs.length];
            final Hit[] hits = new Hit[outs.length];
            int total = 0;
            int nonEmpty = 0;
            Hits res = newHits();
            res.clearUserData();
            for (int i = 0; i < outs.length; i ++) {
                boolean succ = outs[i] != null && 
                        outs[i].readVInt() == CODE_SUCC;
                int size = 0;
                if (succ) {
                    /*
                     * Read user-data
                     */
                    res.appendUserData(outs[i]);
                    /*
                     * Read total
                     */
                    int t = outs[i].readVInt();
                    total += t;
                    /*
                     * Read size
                     */
                    size = outs[i].readVInt();
                    if (size > 0) {
                        hits[i] = newHit();
                        hits[i].readFields(outs[i]);
                    } // if
                } // if
                remains[i] = size;
                if (size > 0)
                    nonEmpty ++;
            } // for i
            
            if (nonEmpty == 0)
                return res;
            
            IntPriorityQueue q = new IntPriorityQueue(nonEmpty,
                new IntComparator() {
                    public int compare(int o1, int o2) {
                        float s1 = hits[o1].getScore(); 
                        float s2 = hits[o2].getScore();
                        if (s1 < s2)
                            return 1;
                        if (s1 > s2)
                            return -1;
                        
                        long d1 = hits[o1].getDocID();
                        long d2 = hits[o2].getDocID();
                        return d1 < d2 ? 1 : d1 > d2 ? -1 : 0;
                    }
                }
            );

            res.setTotal(total);
            // add first element of each out
            for (int i = 0; i < remains.length; i ++) {
                if (remains[i] > 0) {
                    q.add(i);
                } // if
            } // for i
            int skipped = 0;
            while (q.size() > 0 && res.size() < len) {
                // get the current best
                int top = q.poll();
                Hit topHit = hits[top];
                // put it into res
                if (skipped < start)
                    skipped ++;
                else
                    res.add(topHit);
                remains[top] --;
                // supplement the next one in <top> slice if any
                if (remains[top] > 0) {
                    // read and add next hit from <top> slice
                    hits[top] = newHit();
                    hits[top].readFields(outs[top]);
                    q.add(top);
                } // if
            } // while
            return res;
        }
        
        /**
         * Performs a distributed search on a specified query, and returns the
         * specified range of hits in order.
         * 
         * @param query  the query to be searched
         * @param start  the index of the first returned Hit
         * @param len  the expected number of hits returned
         * @param params  the parameters
         * @param ttl  the time-to-live in milli-seconds
         * @param error  the error instance. After return, this object is set
         *               to represent the error-status of this search.
         * @param hitsPerLs  the number of hits per local-search should return
         * @return  the searched Hits if succ. This could be null when some
         *          exception was caught.
         * @throws InterruptedException  if the current thread was interrupted
         */
        public Hits search(String query, int start, int len, 
                Map<String, String[]> params, int ttl, ServiceError error,
                int hitsPerLs) throws InterruptedException {
            
            DataBuffer in = new DataBuffer();
            try {
                StringWritable.writeString(in, query);
                in.writeVInt(hitsPerLs);
                ParameterWritable.writeParameters(in, params);
                DataBuffer[] out = master.processForAll(serviceType, in, ttl, 
                        error);
                if (!error.isSucc())
                    return null;
                else
                    return mergeOut(out, start, len, params);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                LOG.error("Exception caught in search", e);
                error.setStatus(ServiceError.STATUS_FAILED);
                error.setReason(ServiceError.REASON_EXCEPTION);
                return null;
            }
        }

        public int delete(Map<String,String[]> params,int ttl) {
        	DataBuffer delIn=new DataBuffer();
        	try {
				ParameterWritable.writeParameters(delIn, params);
			} catch (IOException e) {
				LOG.error("Exception caught in delete", e);
				return 0;
			}
			int delResult[]=master.deleteProcessForAll(serviceType,delIn,ttl);
			for(int delI:delResult) {
				if(delI==0) {//只有当所有的LS都删除索引成功,才返回1,否则返回0
					return 0;
				}
			}
        	return 1;
        }
        
        public class HitsFuture {
            EntriesFuture res;
            ServiceError error;
            int start;
            int len;
            Map<String, String[]> params;
            
            public HitsFuture(EntriesFuture res, ServiceError error,
                    int start, int len, Map<String, String[]> params) {
                this.res = res;
                this.error = error;
                this.start = start;
                this.len = len;
                this.params = params;
            }
            
            public Hits get() throws InterruptedException {
                DataBuffer[] out = res.get();
                if (!error.isSucc())
                    return null;
                else {
                    try {
                        return mergeOut(out, start, len, params);
                    } catch (Exception e) {
                        LOG.error("Exception caught in search", e);
                        error.setStatus(ServiceError.STATUS_FAILED);
                        error.setReason(ServiceError.REASON_EXCEPTION);
                        return null;
                    }
                } // else
            }
        }
        /**
         * Performs a distributed search on a specified query, and returns the
         * specified range of hits in order in an asynchronized way.
         * 
         * @param query  the query to be searched
         * @param start  the index of the first returned Hit
         * @param len  the expected number of hits returned
         * @param params  the parameters
         * @param ttl  the time-to-live in milli-seconds
         * @param error  the error instance. After return, this object is set
         *               to represent the error-status of this search.
         * @param hitsPerLs  the number of hits per local-search should return
         * @return  the searched Hits if succ. This could be null when some
         *          exception was caught.
         * @throws InterruptedException  if the current thread was interrupted
         */
        public HitsFuture asyncSearch(String query, int start, int len, 
                Map<String, String[]> params, int ttl, ServiceError error,
                int hitsPerLs) {
            DataBuffer in = new DataBuffer();
            try {
                StringWritable.writeString(in, query);
                in.writeVInt(hitsPerLs);
                ParameterWritable.writeParameters(in, params);
                EntriesFuture res = master.asyncProcessForAll(
                        serviceType, in, ttl, error);
                if (res == null)
                    return null;

                return new HitsFuture(res, error, start, len, params);
            } catch (Exception e) {
                LOG.error("Exception caught in search", e);
                error.setStatus(ServiceError.STATUS_FAILED);
                error.setReason(ServiceError.REASON_EXCEPTION);
                return null;
            }
        }
    }
}
