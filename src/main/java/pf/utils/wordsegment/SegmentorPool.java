package pf.utils.wordsegment;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import pf.utils.XMLUtil;


/**
 * A wordsegment pool with capacity limited in constructor for easy using in
 * multi-threading you can create with the pool size Example: SegmentorPool pool =
 * new SegmentorPool(3); CSegmentor seg= pool.take(); seg.doSegment("xxx");
 * pool.put(seg);
 * 
 * @author changwei
 */
public class SegmentorPool {
	public static final Logger LOG=Logger.getLogger(SegmentorPool.class.getName());
	public static final int DEFAULT_TEXT_BUFFER_SIZE = 256 * 1024;

    protected LinkedBlockingQueue<Segmentor> segmentorPool = null;

    /**
     * creat the pool with capability limited
     * 
     * @param capacity
     *            the maxium segmentor object
     * @exception SegmentorException
     *                if cannot construct SegmentorPool object, by reason of: 
     *                CSegmentor initialization failure, or incapability to 
     *                construct CSegmentor object.
     */
    public SegmentorPool(int capacity) {
        this(capacity, null);
    }
    

    /**
     * Create the pool with capability limited
     * 
     * @param capacity,
     *            the maxium segmentor object
     * @param size,
     *            the maxium input text size of each segmentor
     * @param libHome,
     *            the lib path of the segmentor object
     * @exception SegmentorException
     *                if cannot construct SegmentorPool object, by reason of: 
     *                CSegmentor initialization failure, or incapability to 
     *                construct CSegmentor object.
     */
    public SegmentorPool(int capacity, String libHome) {
        
        segmentorPool = new LinkedBlockingQueue<Segmentor>(capacity);
        IKSegmentor.init();
        
        for (int i = 0; i < capacity; i++) {
        	Segmentor seg = null;
            seg = new IKSegmentor();
            
            segmentorPool.offer(seg);
        }
    }

    /**
     * Retrieves a segmentor object, waiting if no elements are present in this
     * pool
     * 
     * @return  one element in the pool if any
     */
    public Segmentor take() {
        // double lock to speed up
        try {
            return segmentorPool.take();
        } catch (InterruptedException e) {
           LOG.error("",e);
            return null;
        }
    }

    /**
     * put back the segmentor object to the pool
     * 
     * @return  one element in the pool if any
     */
    public void put(Segmentor segmentor) {
        try {
            segmentorPool.put(segmentor);
        } catch (InterruptedException e) {
           LOG.error("",e);
        }
    }

    /** 
     * Retrieves  a segmentor object, return null if this pool is empty.
     * 
     * @return  one element in the pool if any 
     */
    public Segmentor poll() {
        return segmentorPool.poll();
    }

    /**
     * Return the number of segmentor in the pool
     * 
     * @return  the number of segmentor in the pool
     */
    public int size() {
        return segmentorPool.size();
    }
}