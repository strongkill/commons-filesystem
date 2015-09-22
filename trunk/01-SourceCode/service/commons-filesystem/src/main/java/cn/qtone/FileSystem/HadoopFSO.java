package cn.qtone.FileSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



/**
 * 云存储基础类，使用方法new HadoopFSO()，后调用相应的方法
 * for local test ,pls modify c:\Windows\System32\drivers\etc\hosts and
 * add following line at the end of this file: 192.168.1.10 namenode-vip
 * 192.168.1.10 namenode-1-vip
 *          
 * @author StrongYuen
 *
 **/


public class HadoopFSO extends Hadoop implements IFSO {

	static final boolean isLocal = false;//代码没有测试通过，暂时不开放
	
	static final String local_dir = "/hadoop_data/album/";// local file path

	static final String basedir = "/data/album"; //保存在HADOOP中的根目录

	static final boolean isCache = false; // 暂时不在这里使用，逻辑放在HadoopActionAction.java中
	
	static final String cache_dir = "/home1/yzt/frontstage/WEB-INF/tmp/"; // local cache file path

	static final String tmpbasedir = System.getProperty("java.io.tmpdir");// "/data/stream";	// local OS TMP PATH;

	static {
		for (String[] shard : getShards()) {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", shard[1]); // conf.set("fs.default.name", "hdfs://namenode-backup-vip:8020");
			conf.set("dfs.block.size", "524288");
			conf.set("dfs.replication", "2");
			conf.set("dfs.permissions", "false");
			conf.set("dfs.permissions.supergroup", "resin");
			conf.set("dfs.web.ugi", "resin");
			try {
				addFS(Integer.valueOf(shard[0]), FileSystem.get(conf));
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Initialize Hadoop Server " + shard[0] + " : "
					+ shard[1]);
		}
	}

	public HadoopFSO() {

	}

	public/* static */Path gethdfsPath(String dir) {
		return this.getPath(dir);
	}	

	public/* static */byte[] getHDFSFile(String dst) throws Exception {
		byte[] buffer = null;

		if (isLocal) {
			buffer= this.readFromLocal(dst);
		} else {
			if (isCache) {
				buffer = this.readFromCached(dst);
			}
			if(buffer==null)
				buffer= this.readFromHadoop(dst);
		}
		return buffer;
	}

	public InputStream getHDFSFileInputStream(String dst) throws Exception {
		if (isLocal) {
			return this.readFromLocalInputStream(dst);
		} else {
			// FileSystem fs = FileSystem.get(conf);
			return this.readFromHadoopInputStream(dst);

		}
	}


	public/* static */String createHdfsFile(String localfilepath)
			throws Exception {
		return this.createHdfsFile(localfilepath, false);
	}


	public/* static */String createHdfsFile(String localfilepath, boolean isdeleteSourcefile) throws Exception {
		String name = this.getHdfsFileName(localfilepath);
		File f = new File(localfilepath);
		FileInputStream fin = new FileInputStream(f);
		String tmp = this.createHdfsFile(name, fin);
		fin.close();
		if (isdeleteSourcefile)
			f.delete();
		f = null;

		return tmp;
	}


	public String createHdfsFile(String filename, File file) throws Exception {
		return this.createHdfsFile(getHdfsFileName(filename), new FileInputStream(
				file));
	}

//
//	public/* static */String createHdfsFile(FormFile f) throws Exception {
//		InputStream fin = f.getInputStream();
//		String tmp = this.createHdfsFile(getHdfsFileName(f.getFileName()), fin);
//		fin.close();
//		return tmp;
//	}


	public/* static */String createHdfsFileFromStream(String dst, InputStream in)
			throws Exception {
		return this.createHdfsFile(dst, in);
	}


	public String createHdfsFileFromStreamWithFormat(String format,
			InputStream in) throws Exception {
		return this.createHdfsFile(getHdfsFileName(format), in);
	}


	public/* static */String createHdfsFilebyte(String format, byte[] in)
			throws Exception {
		return this.createHdfsFile(getHdfsFileName(format), in);
	}


	public boolean exists(String dst) throws Exception {
		return (super.getFs(HadoopFSO.getShard(dst)).exists(this.getPath(dst)));
	}

	public/* static */boolean delHdfsFile(String filepath) throws Exception {
		return this.deleteHdfsFile(filepath);
	}


	public/* static */String createAutoDir() throws Exception {
		String dir = Hadoop.createAutoHadoopDir();
		if (isLocal) {
			this.createlocaldir(dir);
		} else {
			this.createdir(dir);
		}
		return dir;
	}	

	
/**
 * 以下是私有函数、将会取消的函数及未测试函数
 * 
 * 
 * 	
 */



	/**
	 * 
	 * @param dst
	 * @return byte[]
	 * @throws Exception
	 */
	private byte[] readFromLocal(String dst) throws Exception {
		byte[] buffer = null;

		InputStream ins = readFromLocalInputStream(dst);
		buffer = new byte[Integer.parseInt(String.valueOf(dst.length()))];
		ins.read(buffer);
		ins.close();
		return buffer;

	}
	/**
	 * 
	 * @param dst
	 * @return InputStream
	 * @throws Exception
	 */
	private InputStream readFromLocalInputStream(String dst)  throws Exception {
		File f = new File(local_dir + dst);
		if (f.exists()) {
			InputStream ins = new FileInputStream(f);
			return ins;
		} else {
			throw new Exception("the file is not found .");
		}
	}


	/**
	 * 
	 * @param dst
	 * @return byte[] 
	 * @throws Exception
	 */
	private byte[] readFromCached(String dst) throws Exception {
		byte[] buffer = null;
		String tmpfilename = dst.replaceAll("/", "_");
		File f = new File(cache_dir + tmpfilename);
		if (f.exists()) {
			InputStream ins = new FileInputStream(f);
			buffer = new byte[Integer.parseInt(String.valueOf(f
					.length()))];
			ins.read(buffer);
			ins.close();
		}
		return buffer;
	}
/**
 * 
 * @param dst
 * @return byte[]
 * @throws Exception
 */
	private byte[] readFromHadoop(String dst) throws Exception {
		byte[] buffer = null;
		Path path = getPath(dst);
		int shards_number = getShard(dst);
		if (getFs(shards_number).exists(path)) {
			FSDataInputStream is = getFs(shards_number).open(path);
			FileStatus stat = getFs(shards_number).getFileStatus(path);
			buffer = new byte[Integer
			                  .parseInt(String.valueOf(stat.getLen()))];
			is.readFully(0, buffer);
			is.close();
			if (isCache) {
				writeToCached(buffer,dst);
			}
			return buffer;
		} else {
			throw new Exception("the file is not found .");
		}
	}

/**
 * 
 * @param dst
 * @return InputStream
 * @throws Exception
 */
	private InputStream readFromHadoopInputStream(String dst)  throws Exception {
		Path path = getPath(dst);
		int shards_number = getShard(dst);
		if (getFs(shards_number).exists(path)) {
			FSDataInputStream is = getFs(shards_number).open(path);
			return is;
		} else {
			throw new Exception("the file is not found .");
		}
	}

	/**
	 * 
	 * @param buffer
	 * @param dst
	 * @throws Exception
	 */
	private void writeToCached(byte[] buffer,String dst) throws Exception {
		String tmpfilename = dst.replaceAll("/", "_");
		File f = new File(cache_dir + tmpfilename);
		OutputStream oins = new FileOutputStream(f);		
		oins.write(buffer);
		oins.flush();
		oins.close();
		f = null;
	}

	/**
	 * 删除hdfs文件
	 * 
	 * @param filepath
	 *            存放在hadoop文件路径
	 * @return boolean 是否删除成功
	 * @throws Exception
	 *             Tested
	 */
	private/* static */boolean deleteHdfsFile(String filepath) throws Exception {
		Path p = getPath(filepath);
		int shards_number = getShard(filepath);
		return deleteDdfsFile(p,shards_number);
	}

	/**
	 * 返回对应目录在hadoop的绝对路径
	 * 
	 * @param dir
	 *            相对目录名
	 * @return Path 在hadoop的绝对路径 Tested
	 */
	private/* static */Path getPath(String dir) {
		return new Path(basedir + dir);
	}

	/**
	 * 建立HADOOP目录
	 * 
	 * @param dir
	 *            要建立的相对目录
	 * @return Path 在hadoop的绝对路径
	 * @throws IOException
	 *             Tested
	 */
	private/* static */Path createdir(String dir) throws IOException {
		return mkdir(dir);
	}

	/**
	 * Locate file MKDIR
	 * 
	 * @param dir
	 * @throws IOException
	 */
	private void createlocaldir(String dir) throws IOException {
		File f = new File(local_dir + dir);
		if (!f.exists()) {
			f.mkdirs();
		}
	}

	/**
	 * 建立HADOOP目录, 包括子目录
	 * 
	 * @param dir
	 *            要建立的相对目录
	 * @return Path 在hadoop的绝对路径
	 * @throws IOException
	 *             Tested
	 */
	private/* static *//* synchronized */Path mkdir(String dir)
			throws IOException {
		Path p = getPath(dir);
		Iterator<Entry<Integer, FileSystem>> iter = fsList.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Map.Entry<Integer, FileSystem> entry = (Map.Entry<Integer, FileSystem>) iter
					.next();
			FileSystem fs = entry.getValue();
			if (!fs.exists(p))
				fs.mkdirs(p);
		}
		return p;
	}


	/**
	 * 删除hadoop目录
	 * 
	 * @param dir
	 *            存放在hadoop的相对路径。
	 * @return boolean 是否删除成功
	 * @throws IOException
	 *             Tested
	 */

	// private /*static*/ boolean deleteDir(String dir) throws IOException {
	// return delDir(dir);
	// }

	/**
	 * 删除hadoop目录
	 * 
	 * @param dir
	 *            存放在hadoop的相对路径
	 * @return boolean 是否删除成功
	 * @throws IOException
	 *             Tested
	 */
	// private /*static*/ boolean delDir(String dir) throws IOException {
	// return deleteDdfsFile(getPath(dir));
	// }

	/**
	 * 删除hdfs文件
	 * @param shards_number 
	 * 
	 * @param filepath
	 *            存放于hadoop的相对路径
	 * @return boolean 是否删除成功
	 * @throws Exception
	 *             Tested
	 * 
	 */
	private/* static */boolean deleteDdfsFile(Path path, int shards_number) throws IOException {
		String name = path.getName();
		if (name.indexOf(".") <= 0)
			return false;
		String p_name = path.getParent().getName();
		if ("data".equalsIgnoreCase(p_name) || "album".equalsIgnoreCase(p_name))
			return false;
		return getFs(shards_number).deleteOnExit(path);
	}

	/**
	 * 获取既定规则的hadoop文件名
	 * 
	 * @param name
	 *            输入的文件名，主要用于获取文件格式
	 * @return String 返回生成的hadoop文件名
	 * @throws Exception
	 *             Tested
	 * 
	 *             判断是否存在album目录，如果存在不自动生成，获取原来的文件名进行更新。主要用于录音合并。
	 * 
	 */
	private/* static */String getHdfsFileName(String name) throws Exception {
		String dir = null;
		String filename = null;
		if (name.indexOf("album") > -1 && name.lastIndexOf(File.separator) > -1) {
			int spos = name.indexOf("album" + File.separator);
			int pos = name.lastIndexOf(File.separator);
			dir = name.substring(spos + 5, pos);

			createdir(dir);
			filename = dir + name.substring(pos, name.length());
		} else if (name.indexOf("streams") > -1
				&& name.lastIndexOf(File.separator) > -1) {
			int spos = name.indexOf("streams" + File.separator);
			int pos = name.lastIndexOf(File.separator);
			dir = name.substring(spos + 7, pos);

			createdir(dir);
			filename = dir + name.substring(pos, name.length());
		} else {
			dir = createAutoDir();
			filename = dir + super.getRamdomHadoopFileName(name);
		}
		return filename;
	}

	
	/**
	 * 保存文件到hdfs
	 * 
	 * @param dst
	 *            String 存放在hadoop的绝对路径
	 * @param in
	 *            bytes 要保存的文件bytes内容
	 * @return String 存放在hadoop的绝对路径
	 * @throws Exception
	 *             Tested
	 */
	private/* static */String createHdfsFile(String dst, byte[] in)
			throws Exception {
		if (isLocal) {
			FileOutputStream out = new FileOutputStream(local_dir + dst);
			out.write(in);
			out.flush();
			out.close();
			out = null;
		} else {
			// FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream out = getFs(getShard(dst)).create(
					getPath(dst));
			out.write(in);
			out.close();
			// getFs().close();
		}
		return dst;
	}

	/**
	 * 保存文件到hdfs
	 * 
	 * @param dst
	 *            String 存放在hadoop的绝对路径
	 * @param in
	 *            InputStream 要保存的文件Stream内容
	 * @return String 存放在hadoop的绝对路径
	 * @throws Exception
	 *             Tested
	 */
	private/* static */String createHdfsFile(String dst, InputStream in)
			throws Exception {
		if (isLocal) {
			FileOutputStream out = new FileOutputStream(local_dir + dst);
			IOUtils.copyBytes(in, out, 256 * 1024);
			out.flush();
			out.close();
			out = null;
		} else {
			// FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream out = getFs(getShard(dst)).create(
					getPath(dst));
			IOUtils.copyBytes(in, out, 256 * 1024);
			// in.close();
			out.close();
			// getFs().close();
		}
		return dst;
	}

	

	@Override
	public boolean SaveToFile(String dst,String localfile) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	

	// ///////////////////////////未测试代码////////////////////////////////////////

	/**
	 * @deprecated
	 */
	private /* static */void listAll(String dir) throws IOException {
		// FileSystem getFs() = FileSystem.get(conf);

		FileStatus[] stats = getFs().listStatus(new Path(dir));

		for (int i = 0; i < stats.length; ++i) {
			if (stats[i].isDir()) {
				// dir
				System.out.println(stats[i].getPath().toString());

				listAll(stats[i].getPath().toString());
			} else {
				System.out.println(stats[i].getPath().toString());
			}
		}
		// getFs().close();
	}

	/**
	 * @deprecated
	 */
	private/* static */void uploadLocalFile2HDFS(String s, String d)
			throws IOException {

		// FileSystem getFs() = FileSystem.get(conf);

		Path src = new Path(s);
		Path dst = new Path(d);

		getFs().copyFromLocalFile(src, dst);

		// getFs().close();
	}

	/**
	 * create a new file in the hdfs. notice that the toCreateFilePath is the
	 * full path and write the content to the hdfs file.
	 *
	 *
	 * @deprecated
	 */
	private /* static */void createNewHDFSFile(String toCreateFilePath,
			String content) throws IOException {

		// FileSystem getFs() = FileSystem.get(conf);

		FSDataOutputStream os = getFs().create(new Path(toCreateFilePath));

		os.write(content.getBytes("UTF-8"));

		os.close();

		// getFs().close();
	}

	/**
	 * read the hdfs file content notice that the dst is the full path name
	 *
	 * @deprecated
	 */
	private /* static */byte[] readHDFSFile(String dst) throws Exception {

		// FileSystem getFs() = FileSystem.get(conf);

		// check if the file exists
		Path path = new Path(dst);
		if (getFs().exists(path)) {
			FSDataInputStream is = getFs().open(path);
			// get the file info to create the buffer
			FileStatus stat = getFs().getFileStatus(path);

			// create the buffer
			byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat
					.getLen()))];
			is.readFully(0, buffer);

			is.close();
			// getFs().close();

			return buffer;
		} else {
			throw new Exception("the file is not found .");
		}
	}

	/**
	 * 列表目录下所有子目录及目录下的文件
	 * 
	 * @param p
	 *            Path 存放在hadoop目录的绝对路径
	 * @return ArrayList<Path> 目录、文件列表
	 * @throws Exception
	 *             Tested
	 * @deprecated
	 */
	private/* static */ArrayList<Path> ListAllDirAndFile(Path p)
			throws Exception {
		ArrayList<Path> ret = new ArrayList<Path>();
		// FileSystem fs = FileSystem.get(conf);
		if (!getFs().exists(p))
			return ret;

		FileStatus[] stats = getFs().listStatus(p);
		if (stats != null && stats.length > 0) {
			for (FileStatus st : stats) {
				if (st.isDir()) {
					ret.add(st.getPath());
					ListAllDirAndFile(st.getPath());
				} else {
					ret.add(st.getPath());
				}
			}
		}
		// getFs().close();
		return ret;
	}

	/**
	 * 列表目录下所有子目录及目录下的文件
	 * 
	 * @param dir
	 * @return
	 * @throws Exception
	 * Tested
	 * @deprecated
	 */
	private/* static */ArrayList<Path> ls(String dir) throws Exception {
		Path p = getPath(dir);
		return ListAllDirAndFile(p);
	}

}
