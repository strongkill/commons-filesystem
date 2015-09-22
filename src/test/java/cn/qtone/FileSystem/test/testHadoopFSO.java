package cn.qtone.FileSystem.test;

import java.io.FileInputStream;
import java.io.InputStream;

import cn.qtone.FileSystem.FSOFactory;
import cn.qtone.FileSystem.IFSO;
import cn.qtone.FileSystem.YZTFS;

public class testHadoopFSO{
	public static void main(String[] args) throws Exception{
		int i=0;
		IFSO fso = FSOFactory.buildFSO(YZTFS.WCSfsotype);
		//fso.delHdfsFile("/201412/02/7a45f9eb-eb68-44d9-b015-d031c9621bad.flv");
		//while(i<10){
			String local_file = "C:\\Users\\Administrator\\Downloads\\f7c27fd6-901d-4258-9215-a01ccd3e96f5.mp4";
			InputStream in = new FileInputStream(local_file);

			String hadoop_file = fso.createHdfsFileFromStream(local_file, in);
			//"/201412/03/e4e0d98e-3783-4105-bf04-c914c95eecc1.flv";
			//fso.createHdfsFile(local_file);
			System.out.println(hadoop_file);
			//Thread.sleep(1000);
			//fso.SaveToFile(hadoop_file, "c:/temp/aaabb"+i+".flv");
			//Thread.sleep(1000);
			//fso.delHdfsFile(hadoop_file);
			/*
			FileOutputStream oins = new FileOutputStream();
			InputStream in = fso.getHDFSFileInputStream(hadoop_file);
			IOUtils.copyBytes(in, oins, 256*1024);*/
			//i=i+1;
		//}
	}
}
