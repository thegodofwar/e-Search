package wanmei.is.daemon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;


public class DataReceiveDaemon {
	private int listenPort;
	private String savePath;
	public static final Logger LOG=Logger.getLogger(DataReceiveDaemon.class.getName());
	/**
	 * 构造方法
	 * 
	 * @param listenPort
	 *            侦听端口
	 * @param savePath
	 *            接收的文件要保存的路径
	 * 
	 * @throws IOException
	 *             如果创建保存路径失败
	 */
	DataReceiveDaemon(int listenPort, String savePath) throws IOException {
		this.listenPort = listenPort;
		this.savePath = savePath;
		File file = new File(savePath);
		if (!file.exists() && !file.mkdirs()) {
			throw new IOException("无法创建文件夹 " + savePath);
		}
	}

	// 开始侦听
	public void start() {
		new ListenThread().start();
	}

	// 网上抄来的，将字节转成 int。b 长度不得小于 4，且只会取前 4 位。
	public static int b2i(byte[] b) {
		int value = 0;
		for (int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			value += (b[i] & 0x000000FF) << shift;
		}
		return value;
	}

	/**
	 * 侦听线程
	 */
	private class ListenThread extends Thread {

		@Override
		public void run() {
			try {
				ServerSocket server = new ServerSocket(listenPort);
				while (true) {
					Socket socket = server.accept();
					new HandleThread(socket).start();
				}
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
	}

	/**
	 * 读取流并保存文件的线程
	 */
	private class HandleThread extends Thread {

		private Socket socket;

		private HandleThread(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				InputStream is = socket.getInputStream();
				readAndSave(is);
			} catch (IOException e) {
				LOG.error("",e);
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					// nothing to do
				}
			}
		}

		// 从流中读取内容并保存
		private void readAndSave(InputStream is) throws IOException {
			String filename = getFileName(is);
			if (filename == null) {
				return;
			}
			int file_len = readInteger(is);
             LOG.info("filelen:"+file_len);
			if (file_len > 1000000) {
				LOG.info("文件长度过长:"+file_len);
				return;
			}
			LOG.info("接收文件：[" + filename + "] 长度：" + file_len);

			readAndSave0(is, savePath + filename, file_len);

			LOG.info("文件保存成功（" + file_len + "字节）。");
		}

		private void readAndSave0(InputStream is, String path, int file_len)
				throws IOException {
			FileOutputStream os = getFileOS(path);
			readAndWrite(is, os, file_len);
			os.close();
		}

		// 边读边写，直到读取 size 个字节
		private void readAndWrite(InputStream is, FileOutputStream os, int size)
				throws IOException {
			byte[] buffer = new byte[4096];
			int count = 0;
			while (count < size) {
				int n = is.read(buffer);
				// 这里没有考虑 n = -1 的情况
				os.write(buffer, 0, n);
				count += n;
			}
		}

		// 读取文件名
		private String getFileName(InputStream is) throws IOException {
			int name_len = readInteger(is);
			if (name_len > 1000000) {
				LOG.info("名称长度过长:"+name_len);
				return null;
			}
			LOG.info("namelen:"+name_len);
			byte[] result = new byte[name_len];
			is.read(result);
			return new String(result);
		}

		// 读取一个数字
		private int readInteger(InputStream is) throws IOException {
			byte[] bytes = new byte[4];
			is.read(bytes);
			return b2i(bytes);
		}

		// 创建文件并返回输出流
		private FileOutputStream getFileOS(String path) throws IOException {
			File file = new File(path);
			if (!file.exists()) {
				file.createNewFile();
			}

			return new FileOutputStream(file);
		}
	}
	
	public static void main(String[] args) {
		
		int port = Integer.parseInt(args[0]);
		String path = args[1];
		try {
			new DataReceiveDaemon(port, path).start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
	}

}
