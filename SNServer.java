//import org.apache.thrift.server.TServer;
//import org.apache.thrift.server.TServer.Args;
//import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.io.*;

public class SNServer {

  public static NodeHandler handler;

  public static NService.Processor processor;
  public static int SNPort=9090, SNId=0;
  public static LinkedList<node> nodeList= new LinkedList<node>();
  public static  Map<String,List<node>> eventual1 = new HashMap<String,List<node>>(); 
    public static  Map<String,Integer> eventual2 = new HashMap<String,Integer>();
    public static  Map<String,String> eventual3 = new HashMap<String,String>();
  //Hardcoded
  public static String SNIp = "csel-x32-01.cselabs.umn.edu";

  public static int nr, nw, nTotal;
  public static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


    public static void replicate() {
        
		Iterator it = eventual1.entrySet().iterator();

		//Loop for each file which has been written but not replicated
        	while(it.hasNext()){
                Map.Entry<String, List<node>> pair=(Map.Entry)it.next();
                //Acquire a writeLock
                lock.writeLock().lock();
                System.out.println("--------------------------------------------");
		System.out.println("Replicating changes for file "+ pair.getKey());
		System.out.println("--------------------------------------------");

		List<node> thisList= pair.getValue();
		//System.out.println("Replicating file: "+ pair.getKey()+ "for nodes:");
		/*for (node n: thisList){
		System.out.print(" " +n.id);
		}*/
                for(node nodeIt: thisList){
                	//RPC call to nodeIt.ip
			requests r= new requests();
			try {
                        TTransport transport;
                         //Taking the ip and port number of the nodes in the list of activeNodes
                        transport = new TSocket(nodeIt.ip, nodeIt.port);
                        TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
                        NService.Client client = new NService.Client(protocol);
                        transport.open();
                        //Reading the latest file from the node
                        //System.out.println("Replicating on "+ nodeIt.id +"for filename:  "+  pair.getKey()+"."+eventual2.get(pair.getKey()));
                        r.status= client.doWrite(pair.getKey()+"."+eventual2.get(pair.getKey()) , eventual3.get(pair.getKey()));
                        if(r.status==1)
 				System.out.println("Successfully replicated "+ pair.getKey()+ "." + eventual2.get(pair.getKey()) + " at node "+ nodeIt.id );
			else 
				System.out.println("Failed to replicate on node: "+ nodeIt.id);

              transport.close();
            } catch (TException x) {
              x.printStackTrace();
            }

                }

                it.remove();
                //Release the lock
                lock.writeLock().unlock();
        }


	//Sleep for sometime
        try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
            	//Handle the exception	
		e.printStackTrace();
            }
		//System.out.println("Came out of sleep");

		//Call recursively
		SNServer.replicate();

	}
   

  public static void main(String [] args) {

    //SNPort= Integer.parseInt(args[0]);	
    
    nr= Integer.parseInt(args[0]);
    nw= Integer.parseInt(args[1]);	
    nTotal= Integer.parseInt(args[2]);
    try {
      handler = new NodeHandler();
      processor = new NService.Processor(handler);

      Runnable simple = new Runnable() {
        public void run() {
          simple(processor);
        }
      };

      new Thread(simple).start();
    } catch (Exception x) {
      x.printStackTrace();
    }


     File file = new File("/export/scratch/kajar_mahal");
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("File Server directory named kajar_mahal is created in /export/scratch");
            } else {
                System.out.println("Failed to create directory!");
            }
        }


    //Take an instance for calling the reolicate function

     try {
     		 Runnable replicate = new Runnable() {
        	public void run() {
	          replicate();
       		}
      };

      new Thread(replicate).start();
    } catch (Exception x) {
      x.printStackTrace();
    }



  }


  

  public static void simple(NService.Processor processor) {
 
    node ncord= new node();
    ncord.id=SNId;
    ncord.port= SNPort;
    ncord.ip= SNIp;
    nodeList.add(ncord);
 
  
   try {
     // System.out.println("Beforeopening socket");
      TServerTransport serverTransport = new TServerSocket(SNPort);
          TTransportFactory factory = new TFramedTransport.Factory();
      TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
      args.processor(processor);
      args.transportFactory(factory);
       // System.out.println("After configuring the port");
        TThreadPoolServer server = new TThreadPoolServer(args);
      System.out.println("Starting the coordinator server...");
      server.serve();
     
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}

