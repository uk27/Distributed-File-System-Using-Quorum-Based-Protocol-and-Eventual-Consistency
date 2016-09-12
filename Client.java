import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.lang.Math.*;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;


public class Client {

   public static List<String> fileNames = Arrays.asList("Java", "Groovy", "Python");
   public static int rc=0, wc=0;
   public static float readTime, writeTime;

   public static void requestOp(int op, String fileName, String content){

      node n1=new node();  
    
   	try {
      		TTransport transport;

	      //To be changes to the IP and Port of the Super Node      
	      transport= new TSocket("csel-x32-01.cselabs.umn.edu", SNServer.SNPort);

	      TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));

	      NService.Client client = new NService.Client(protocol);

	      transport.open();
	      n1=client.giveRandomNode();
	      //System.out.println("Random Node returned= "+ n1.ip+ ":" + n1.port + ":" + n1.id);

	      transport.close();
	    } catch (TException x) {
	      x.printStackTrace();
	    }

	 try {
	       TTransport transport;
	      //Taking the ip and port number of the nodes in the list of activeNodes
	       transport = new TSocket(n1.ip, n1.port);
	       TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
	       NService.Client client = new NService.Client(protocol);
	       transport.open();
	
	       if(op==1){
		   //incrementing counter for write op	
		   //wc++;
		   final long startTime = System.currentTimeMillis();
		   requests r2= client.requestWrite(fileName, content, n1.id);
	           final long endTime = System.currentTimeMillis();
		   wc++;
		   //Adding time of this operation to the total writeTime
		   writeTime+= endTime-startTime;
		   if(r2.status==1)
	              	System.out.println("\nWrite successful!\n"+ r2.contents);
	       }
		else if (op==0){
		//Incrementing counter for read op
		//rc++;
		final long startTime = System.currentTimeMillis();
		requests r2= client.requestRead(fileName,n1.id);
		final long endTime = System.currentTimeMillis();
		rc++;
		//Adding duration of this op to the total readTime
		readTime= (endTime- startTime);
                if(r2.status==1)
                	System.out.println("\nRead successful! \n Content is: " + r2.contents);
		else if(r2.status==0)
			System.out.println("\nRead Unsuccessful\n");
		} 
 
	      transport.close();
	    } catch (TException x) {
	      x.printStackTrace();
	    }

	  }


	public static void main(String args[]) {
	
		final int op= Integer.parseInt(args[0]);
		final int numberOfOps= Integer.parseInt(args[1]);

		//if op=0 , then read heavy client. If op=1, then write heavy client
		//if op=2 then equal number of reads and write
		//if op=3, print state of the file System
		if(op ==0 || op==1){
			for(int i=1; i<= numberOfOps; i++){
			final int t=i; 	
        			try {
					//int r;
                			Runnable requestOp = new Runnable() {
               				public void run() {
					if(op==0){
						
						final int r = (t%10==0)?1:0;
						//Uncommenting the below line and commenting the above line gives negative test case
						//final int r= 0;
						requestOp(r, fileNames.get(t%3), fileNames.get(t%3));
					}
					else{
						final int r=(t%10==0)?0:1;
					//final int r=1;
                			requestOp(r, fileNames.get(t%3), fileNames.get(t%3));
                			}
					}
                		};
				new Thread(requestOp).start();
            			} catch (Exception x) {
                      			x.printStackTrace();
            			}
        		}
		}
		if(op==2){
			for(int i=1; i<= numberOfOps; i++){
                        final int t=i;  
                                try {
                                        Runnable requestOp = new Runnable() {
                                        public void run() {
                                        requestOp(t%2, fileNames.get(t%3), "Hello World"+fileNames.get(t%3));
                                        }
                                };      
                                new Thread(requestOp).start();
                                } catch (Exception x) {
                                        x.printStackTrace();
                                }
                        }
			
		}
		if(op==3){
			try {
                		TTransport transport;

              			//To be changes to the IP and Port of the Super Node      
              			transport= new TSocket("csel-x32-01.cselabs.umn.edu", SNServer.SNPort);

              			TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));

              			NService.Client client = new NService.Client(protocol);

              			transport.open();
				 Map<Integer, List<String>> fileSystemState =client.getFilesOnSystem();
				

				for (Map.Entry<Integer, List<String>> entry : fileSystemState.entrySet()) {
  					Integer nodeId = entry.getKey();
  					List<String> listOfFilesOnNode = entry.getValue();
					System.out.println("\nThe files on the node " + nodeId.intValue() + " are: ");
					for(String file : listOfFilesOnNode ){
						System.out.print(file + " ");
					}
					System.out.println();
				}

              			transport.close();
            		} catch (TException x) {
              		x.printStackTrace();
            		}

		}
		if(op==0 || op==1 || op==2){
		while(rc+wc < numberOfOps){

			try{
                       Thread.sleep(1);
                }catch(Exception e){
                     e.printStackTrace();
                }
		//System.out.println("rc+wc is:"+ (rc+wc));

		}
		System.out.println("Total reads: "+ rc);//+ " Average read time: " + (readTime/rc)+ " "+ numberOfOps);
		System.out.println("Total writes: "+ wc);
		System.out.println("Total time: " + writeTime+readTime);
	      }
	}
}
