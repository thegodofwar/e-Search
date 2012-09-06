package wanmei.mina.handler;

import java.util.Map;

import outpost.search.Hits;

/**
 * An abstract local-index searcher class
 * 
 * @author David
 *
 */
public abstract class LocalIndexSearcher {
    /**
     * Search some results (starts with <start>, maximum <len> hits) for
     * a specified query and parameters.
     * 
     * The type of an element in Hits can be a subclass of Hits.Hit, but the
     * corresponding modification needs to be made on the client. e.g. override
     * the IndexSearchHandlerAdapters.IndexSearcher.newHit() to return a 
     * different new instance of subclass of Hits.Hit. 
     * 
     * @param query  the query to be searched
     * @param start  the positition of the first hit returned
     * @param len  the maximum number of hits returned
     * @param params  the parameters
     * @return  the search result as a Hits object.
     *          NOTE: DO NOT return null even if no results are found, return
     *          an empty Hits instead.
     */
    public abstract Hits search(String query, int start, int len, 
            Map<String, String[]> params);
    
    /**
     * 
     * @return delete result,if delete index successfully,then 1;else 0
     */
    public abstract int delete(Map<String,String[]> params);
    
}

