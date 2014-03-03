package uc;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;


public class UserClient {

	/*
	 * (1)待master和cache都准备就绪后，即可启动。
	 * (2)初次启动后，首先向master询问给自己分配的目标存储节点地址。
	 * (3)收到目标存储节点后，即向cache server发送流式数据。
	 * (4)从第一次向master询问之后，每6分钟和master确认一次目标存储节点。
	 * (5)若目标存储节点正常，则可继续发送数据；若目标节点不正常，则重新请求新的目标存储节点，回到（2）。
	 * (6)...
	 */
	
	static String masterIP = "172.18.183.156";
	static String targetIP;
	
	public static void main(String[] args) throws InterruptedException {
		// 向master询问目标存储节点地址
		TargetNode target = new TargetNode();
		targetIP = target.ucTargetNode(masterIP);
		
		String name = "/home/ello/Documents/kugou/play";
		String userID[] = {"ello","mary","lily","suzan","frank"};

		int i = 1;
		long amount = 10;
		
		
		// 开启定时与master确认当前cache服务器是否正常的任务
	    final Timer timer1 = new Timer();
		TimerTask ackCacheTask = new TimerTask() {
			int count = 1;
		    String ackFromM;
			public void run() {
				System.out.println("客户端: 第 " + count + " 次和Master确认cache节点状态！");
				AckFromMaster ackFromMaster = new AckFromMaster();
				System.out.println("等待master返回确认信息......");
				try {
					ackFromM = ackFromMaster.ucAckCacheIP(masterIP, targetIP);
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println("master返回确认信息： " + ackFromM);
				if( ackFromM == null ){
					System.out.println("The target cache is not changed, the IP is : " + targetIP);
				}
				else if(ackFromM.equals("OK") || ackFromM.equals(targetIP)){
					System.out.println("The target cache is OK,or no better cache than : " + targetIP);
				}	
				else if(ackFromM.equals("NO")){
					System.out.println("There is no more good cache node !");
				}			
				else {
					targetIP = ackFromM;
				    System.out.println("The new target cache IP is : " + targetIP);
				}			
				count++;
				if(count > 100){
					System.out.println("timer1:ack cache server state with master canceled.");
					this.cancel();
					timer1.cancel();
				}
			}
		};	
		// 设计定时器，1000毫秒后启动计时器任务，每隔1000*5毫秒再启动一次
		long startTime1 = 1000;
		long interval1 = 1000 * 10;
		timer1.schedule(ackCacheTask, startTime1, interval1);  // user client每隔一定时间和master确认当前cache节点状态
		

		SendStreamData sendData = null;
		while(i <= 20){			
			if(targetIP!=null && !targetIP.equals("error")){
			sendData = new SendStreamData(name+i,targetIP,userID[i%5],amount,masterIP);
			sendData.start();
			System.out.println("client " + i + " start ");
			i++;
			}		

		}		
		System.out.println("Totally start " + (i-1) + " clients ");

	}

}
