package cn.qtone.FileSystem;

import java.io.File;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;



public interface IFSO {
	/**
	 * 保存文件到hdfs
	 * 
	 * @param format
	 *            String 文件格式说明，可以直接输入文件名；如abc.jpg、.mp4、.flv
	 * @param in
	 *            输入文件内容的Stream
	 * @return String 保存在hadoop内部的文件路径
	 * @throws Exception
	 *             Tested
	 */
	String createHdfsFileFromStreamWithFormat(String format,
			InputStream in) throws Exception;
	
	/**
	 * 保存文件到hdfs
	 * 
	 * @param format
	 *            String 文件格式说明，可以直接输入文件名；如abc.jpg、.mp4、.flv
	 * @param in
	 *            byte[] 输入的byte[]文件内容
	 * @return String 保存在hadoop内部的文件路径
	 * @throws Exception
	 *             Tested
	 */
	String createHdfsFilebyte(String format,byte[] in) throws Exception;
	/**
	 * 保存文件到hdfs,不建议使用，
	 * 
	 * @param dst
	 *            hadoop存放的路径
	 * @param in
	 *            输入的文件InputStream
	 * @return String 存放在hadoop的路径
	 * @throws Exception
	 *             Tested
	 */	
	String createHdfsFileFromStream(String dst,InputStream in) throws Exception;


	/**
	 * 保存文件到hdfs
	 * 
	 * @param filename
	 *            要保存文件名，主要用于获取文件格式
	 * @param f
	 *            要保存的File对象
	 * @return String 保存在hadoop内部的文件路径
	 * @throws Exception
	 *             Tested
	 */	
	String createHdfsFile(String filename,File file) throws Exception;
	/**
	 * 保存文件到hdfs，是否删除源文件
	 * 
	 * @param localfilepath
	 *            本地文件路径
	 * @param isdeleteSourcefile
	 *            是否删除源文件
	 * 
	 * @return 保存在hadoop内部的文件路径
	 * @throws Exception
	 *             Tested
	 */
	String createHdfsFile(String localfilepath,boolean isdeleteSourcefile) throws Exception;
	/**
	 * 保存文件到hdfs
	 * 
	 * @param localfilepath
	 *            本地文件路径
	 * @return 保存在hadoop内部的文件路径
	 * @throws Exception
	 *             Tested
	 */	
	String createHdfsFile(String localfilepath) throws Exception;
	/**
	 * 删除hdfs文件
	 * 
	 * @param filepath
	 *            String 存放在hadoop中的相对路径
	 * @return boolean 是否删除成功
	 * @throws Exception
	 *             Tested
	 */	
	boolean delHdfsFile(String filepath) throws Exception;
	
	/**
	 * 获取hdfs文件内容
	 * 
	 * @param dst
	 *            String 存放在hadoop中的相对路径
	 * @return InputStream 文件内容
	 * @throws Exception
	 *             Tested
	 */
	InputStream getHDFSFileInputStream(String dst) throws Exception;
	
	
	/**
	 * 保存文件到本地路径
	 * @param dst
	 * @param localfile
	 * @return
	 * @throws Exception
	 */
	boolean SaveToFile(String dst,String localfile) throws Exception;
	
	
	/**
	 * 判断dst是否在HDFS中存在
	 * 
	 * @param dst
	 *            String 存放在hadoop中的相对路径
	 * @return boolean true 存在 false 不存在
	 * @throws Exception
	 *             Tested
	 */
	boolean exists(String dst) throws Exception;
	
	/**
	 * 获取hdfs文件内容
	 * 
	 * @param dst
	 *            String 存放在hadoop中的相对路径
	 * @return byte[] 文件内容
	 * @throws Exception
	 *             Tested
	 */
	byte[] getHDFSFile(String dst) throws Exception;
	/**
	 * 生成以yyyymm/dd的目录格式，并在hadoop中建立。
	 * 
	 * @return String 返回在hadoop的相对路径名称
	 * @throws Exception
	 *             Tested
	 */
	String createAutoDir() throws Exception ;

	/**
	 * 返回对应目录在hadoop的绝对路径
	 * 
	 * @param dir
	 *            相对目录名
	 * @return Path 在hadoop的绝对路径 Tested
	 */
	Path gethdfsPath(String dir);
}
