package uc;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;


/*
 * UserClient ask the Master the target cache node IP , Port:5600
 * UserClient ack with Master in a interval , Port:5800
 * check the RespondToClient class in MasterServer project
 */

public class TargetNode {

	private static Socket connectToMaster;
	
	private String askType = "askIP";

	public static void main(String[] args) {
	
	}
	
	// Get the local IP
	public static String ucGetLocalIP() {
		try {
		InetAddress ind = InetAddress.getLocalHost();
		System.out.println("The client IP is : " + ind.getHostAddress().toString());
		return ind.getHostAddress().toString();
		} catch(IOException e) {
			System.out.println("Can't get the local client IP !");
			return null;
		}	
	}
	
	// ask the Master for the target cache node
	public String ucTargetNode(String masterIP) {
		try {
			connectToMaster = new Socket(masterIP,5600);
			DataInputStream fromMaster = new DataInputStream(connectToMaster.getInputStream());
			DataOutputStream outToMaster = new DataOutputStream(connectToMaster.getOutputStream());
			outToMaster.writeUTF(askType);
			outToMaster.flush();
			System.out.println("Send the asking request to Master !");
			System.out.println("Connection from Master：" + connectToMaster.getInetAddress().getHostAddress());
			String targetIP = null;
			targetIP = fromMaster.readUTF();
			if(!targetIP.equals("error")){
				System.out.println("The target cache node IP is ：" + targetIP);
			}else {
				System.out.println("Master respond error, no good CacheNode to use, please wait the scaling out...");
			}
			fromMaster.close();
			outToMaster.close();
			connectToMaster.close();
			return targetIP;
			
		} catch(IOException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
}
