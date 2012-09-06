package wanmei.is.kv;

import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class TestJni {
	
	static {
		System.loadLibrary("xerces-c-3.1");
		System.loadLibrary("xerces-c");
		
		System.loadLibrary("kv_client");
		
		
	}
	
    private native int kv_client_init(String filename);
	
	
    private native int kv_client_destroy();
	
    private native int kv_client_read(long key, int if_spam, PointerByReference mpp);
	
    private native void release_data_buffer(Pointer mp);
    
    
    public void test(){
    	kv_client_init("example.xml");
    }

}
