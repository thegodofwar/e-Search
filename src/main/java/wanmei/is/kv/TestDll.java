package wanmei.is.kv;

import wanmei.is.data.EmlMeta;
import wanmei.is.kv.KVclient.kv_client_mem;
import wanmei.is.parser.MailParser;
import wanmei.is.parser.PooledMailParser;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class TestDll {
	
	
	public static void main(String[] args) {
		
		
		System.out.println(System.getProperty("user.dir"));
		Native.loadLibrary("xerces-c-3.1", KVclient.class);
		Native.loadLibrary("xerces-c", KVclient.class);
		KVclient instance = (KVclient) Native.loadLibrary("kv_client", KVclient.class);
		/*int res= instance.kv_client_init("example.xml");
		System.out.println("init:"+res);
		
				
		PointerByReference pp = new PointerByReference();
		

		
		int rres = instance.kv_client_read(4294967302L, 0, pp);
		
		Pointer p = pp.getValue();
		

		kv_client_mem mem = new kv_client_mem();
		System.out.println("size:"+mem.size());
		mem = new kv_client_mem(p);
		
		mem.read();
		
		
		
		System.out.println("read:"+rres+" string:"+ mem.size()+" "+mem.mem_size);
		
		
		
		instance.release_data_buffer(pp.getValue());*/
		
		System.out.println("stop relase");
//		PooledMailParser parser = new PooledMailParser();
		System.out.println("new parse");
		EmlMeta meta = MailParser.parseMessage(4295369817L, false);
		
		System.out.println(meta.getTxtBody());
		
//		parser.close();
		
//		instance.kv_client_destroy();
//		System.out.println("finished "+res);
		 
	}

}
