package uc;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/*
 * UserClient get the acknowlegdement from master
 */

public class AckFromMaster {

	private static Socket connectToMaster;
	
	private static String ackType = "ack";
	
	private static String ackFromMaster1 = "OK";
	private static String ackFromMaster2 = "NO";
	
	
	
	// ack with Master to check the current cache state
	public String ucAckCacheIP(String masterIP,String targetIP) throws IOException{
		String ackFromMaster = null;
		
		try {
			connectToMaster = new Socket(masterIP,5800);
			DataInputStream fromMaster = new DataInputStream(connectToMaster.getInputStream());
			DataOutputStream outToMaster = new DataOutputStream(connectToMaster.getOutputStream());
			
			System.out.println("ack with Master to check the cache state...");
			outToMaster.writeUTF(ackType);
			outToMaster.writeUTF(targetIP);
			outToMaster.flush();
			System.out.println("connection from Master :" + connectToMaster.getInetAddress().getHostAddress());
			
			ackFromMaster = fromMaster.readUTF();
			
			if(ackFromMaster==null){
				System.out.println("Not receive ack info from Master!");
				ackFromMaster = ackFromMaster2;
			}
			else if(ackFromMaster.equals(ackFromMaster1)){
				System.out.println("The current Cache node is Ok! Go send data !");
			}
			else if(ackFromMaster.equals(ackFromMaster2)) {
				System.out.println("Master has no apporiate cache node to use !");
			}
			else {
				System.out.println("The target cache node of local client is adviced to change :" + ackFromMaster);
			}
			outToMaster.writeUTF("OK");
			outToMaster.flush();
			fromMaster.close();
			outToMaster.close();
			connectToMaster.close();
			return ackFromMaster;
			
		} 
		catch(IOException e) {
			e.printStackTrace();			
			return null;
		} finally {
			connectToMaster.close();
		}

	}


}
