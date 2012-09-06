package wanmei.is.kv;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;


public interface KVclient extends com.sun.jna.Library {


	
	public int kv_client_init(String filename);
	
	
//	public int kv_client_destroy();
	
	public int kv_client_read(long key, int if_spam, PointerByReference mpp);
	
	public void release_data_buffer(Pointer mp);
//	public void release_data_buffer(Memory pointer);	
	
	public class kv_client_mem extends Structure{
		
		
		
		public kv_client_mem(Pointer p) {
			super(p);
		}
		
		public kv_client_mem() {
			super();
		}

//        public static class ByValue extends kv_client_mem implements Structure.ByValue{ }
		
		public Pointer mem_ptr;
		public int mem_size;
		/*public short mem_kv_type;
		public short mem_handler;
		public int mem_kv_id;*/
		
		
		
		
		
	}



	
}
