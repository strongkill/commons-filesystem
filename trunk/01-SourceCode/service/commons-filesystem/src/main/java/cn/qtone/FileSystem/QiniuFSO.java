package cn.qtone.FileSystem;

import java.io.File;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;


public class QiniuFSO extends Qiniu implements IFSO{

	@Override
	public String createHdfsFileFromStreamWithFormat(String format,
			InputStream in) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String createHdfsFilebyte(String format, byte[] in) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String createHdfsFileFromStream(String dst, InputStream in)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
//
//	@Override
//	public String createHdfsFile(FormFile f) throws Exception {
//		// TODO Auto-generated method stub
//		return null;
//	}

	@Override
	public String createHdfsFile(String filename, File file) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String createHdfsFile(String localfilepath,
			boolean isdeleteSourcefile) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String createHdfsFile(String localfilepath) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean delHdfsFile(String filepath) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public InputStream getHDFSFileInputStream(String dst) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean exists(String dst) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte[] getHDFSFile(String dst) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String createAutoDir() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path gethdfsPath(String dir) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean SaveToFile(String dst, String localfile) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

}
