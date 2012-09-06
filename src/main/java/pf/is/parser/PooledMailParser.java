package pf.is.parser;

import java.util.concurrent.LinkedBlockingQueue;


import wanmei.is.data.EmlMeta;

public class PooledMailParser {
	private static final int DEF_QUERY_PARSER_POOL_SIZE = 1;
    
    private LinkedBlockingQueue<MailParser> pool = new LinkedBlockingQueue<MailParser>();
    
    public PooledMailParser() {
        for (int i = 0; i < DEF_QUERY_PARSER_POOL_SIZE; ++i) {
            pool.offer(new MailParser());
        }
    }
    
    public EmlMeta parse(long key, boolean isSpam) {
        MailParser parser = null;
        while (parser == null) {
            try {
                parser = pool.take();
            } catch (InterruptedException e) {
                
            }
        }
        try {
            return parser.parseMessage(key, isSpam );
            
        } finally {
            pool.offer(parser);
        }
    }
    
    public void close() {
    	for (MailParser parser : pool) {
    		parser.close();
    	}
    }

}
