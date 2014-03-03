package uc;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;



/*
 * UserClient读取文本信息
 * 并将数据发送给对应的CacheServer
 * 对应CacheServer中的ReceiveDataThread和ReceiveStreamData
 */


public class SendStreamData extends Thread{
	
	private String fileName;  // 数据来源
	private String cacheIP;   //  目标存储节点地址
	private String userID;
	private long amount; // 每次批量传送的数据量
	
	public SendStreamData(String fileName,String cacheIP,String userID,long amount,String masterIP){
		this.fileName = fileName;
		this.cacheIP = cacheIP;
		this.userID = userID;
		this.amount = amount;
	}
	
	public void run(){	
		
		ucSendStreamData(userID);

	}
	
	public void ucSendStreamData(String userID) {
		String sendIP = this.cacheIP;
		File file = new File(fileName);
		BufferedReader reader = null;
		long timeStamp;                //    以用户ID加时间戳(userID + "-" + timeStamp)作为Redis中的key
		String tempData;
		String signal = "EndInput";

		try {			
			Socket connectToCache = new Socket(sendIP,5900);
			DataOutputStream toCache = new DataOutputStream(connectToCache.getOutputStream());
			System.out.println("Starting send streaming data to cache server...");

			reader = new BufferedReader(new FileReader(file));
			int count = 1;
			
			 while(count <= amount && sendIP.equals(this.cacheIP)) {
				 if((tempData=reader.readLine())!=null) {
					 timeStamp =  System.currentTimeMillis();
					 toCache.writeUTF(userID);
					 toCache.writeLong(timeStamp);
					 toCache.writeUTF(tempData);
					 toCache.flush();					 
					 count++;
				 } else {
					 System.out.println("There is no data to send !");					 
					 break;
				 }
			 }            // 若cacheIP改变则关闭原socket连接，与新返回的cache节点建立连接
			 
			 System.out.println(this.getName() + " finished sending data ! Count = " + (count-1));
			 toCache.writeUTF(signal);
			 toCache.flush();
			 toCache.close();
			 connectToCache.close();	
			
		} catch(IOException e) {
			e.printStackTrace();
		}
		finally {

		}
	}

}
