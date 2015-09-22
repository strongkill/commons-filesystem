package cn.qtone.FileSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.fs.Path;



import cn.qtone.FileSystem.entry.MyRet;

public class WcsFSO extends WCS  implements IFSO{


	@Override
	public String createHdfsFilebyte(String format, byte[] in) throws Exception {
		InputStream ins = new ByteArrayInputStream(in);
		return createHdfsFileFromStreamWithFormat(format,ins);
	}

	@Override
	/**
	 * Tested
	 */
	public String createHdfsFileFromStream(String dst, InputStream in)
			throws Exception {
		MyRet ret = upload(dst, in);
		return ret.fname;
	}

	@Override
	/**
	 * Tested
	 */
	public String createHdfsFileFromStreamWithFormat(String format,
			InputStream in) throws Exception {
		MyRet ret = upload(format, in);
		return ret.fname;
	}
	
//	@Override
//	/**
//	 * Tested
//	 */
//	public String createHdfsFile(FormFile f) throws Exception {
//		InputStream fin = f.getInputStream();
//		MyRet ret = upload(f.getFileName(), fin);
//		fin.close();
//		return ret.fname;
//	}

	@Override
	/**
	 * Tested
	 */
	public String createHdfsFile(String filename, File file) throws Exception {
		return createHdfsFileFromStream(filename, new FileInputStream(
				file));
	}

	@Override
	/**
	 * @tested
	 * 
	 */
	public String createHdfsFile(String localfilepath,
			boolean isdeleteSourcefile) throws Exception {
		MyRet ret = upload(localfilepath);
		if(isdeleteSourcefile)
			new File(localfilepath).delete();
		return ret.fname;
	}


	@Override
	/**
	 * Tested
	 */
	public boolean SaveToFile(String dst, String localfile) throws Exception {
		MyRet ret = download(dst, localfile);
		return ret!=null && ret.error_code==200;
	}

	
	@Override
	/**
	 * @tested
	 */
	public String createHdfsFile(String localfilepath) throws Exception {
		return createHdfsFile(localfilepath,false);
	}

	@Override
	/**
	 * Tested
	 */
	public boolean delHdfsFile(String filepath) throws Exception {
		MyRet ret = delete(filepath);
		return ret!=null && ret.error_code==200;
	}

	@Override
	/**
	 * @deprecated
	 */
	public InputStream getHDFSFileInputStream(String dst) throws Exception {
		// not implement
		return null;
	}

	@Override
	/**
	 * @deprecated
	 */
	public boolean exists(String dst) throws Exception {
		// not implement
		return false;
	}

	@Override
	/**
	 * @deprecated
	 */
	public byte[] getHDFSFile(String dst) throws Exception {
		// not implement
		return null;
	}

	@Override
	/**
	 * WCS system create dir auto
	 * @deprecated
	 */
	public String createAutoDir() throws Exception {
		// WCS system create dir auto
		return null;
	}

	@Override
	/**
	 * WCS system havent Paht
	 */
	public Path gethdfsPath(String dir) {
		// not implement
		return null;
	}

	
}
