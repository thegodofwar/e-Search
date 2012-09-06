package pf.mina.handler;

import outpost.service.DataBuffer;

/**
 * The interface for handling each search request.
 * 
 * @author liufukun
 *
 */
public interface IHandler {
    /**
     * Execute an handling search request and returns the search result.
     * 
     * @param in  the input data
     * @return  the output data
     */
    public DataBuffer exec(DataBuffer in);
}
