package wanmei.mina.handler;

import outpost.service.DataBuffer;

/**
 * The interface for handling each delete request.
 * 
 * @author liufukun
 *
 */
public interface DeleteIHandler {
	 /**
     * Execute an handling delete request and returns the delete result.
     * 
     * @param in  the input data
     * @return  the output data
     */
    public DataBuffer exec(DataBuffer in); 
}
