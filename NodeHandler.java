import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import  java.util.concurrent.TimeUnit;
import java.lang.Math.*;

import org.apache.thrift.transport.*;

import org.apache.thrift.server.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.Charset;



public class NodeHandler implements NService.Iface
{
	//public static LinkedList<node> nodeList= new LinkedList<node>();

	@Override
        public boolean ping() throws TException {return true;}
	
	//Always served by co-ordinator
	@Override          
	public node giveRandomNode(){
	
	Random r = new Random();
        int low = 0;
	int high = SNServer.nodeList.size()-1;


        return SNServer.nodeList.get(r.nextInt((high-low)+1) + low);
	}

	@Override
	public  requests requestWrite(String fileName, String content, int nodeId){
	
	//System.out.println("\n\nWrite requested on node: "+ nodeId );
	requests r1= new requests();

        //If this node is the coordinator itself
        if(nodeId== SNServer.SNId)
        {
                //System.out.println("Write on coordinator node");
                r1=coordinatorFunc(fileName, 1,content);
        }
        else
        {
                //RPC call to coordinator
        try {
                TTransport transport;

                //Taking the ip and port number of the nodes in the list of activeNodes
                transport = new TSocket(SNServer.SNIp, SNServer.SNPort);

                TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
                NService.Client client = new NService.Client(protocol);

                transport.open();

                //System.out.println("Calling coordinator fn from this node");

                r1= client.coordinatorFunc(fileName, 1, content);

                //System.out.println("Back from cordinatorfn");

                transport.close();
              } catch (TException x) {
                  x.printStackTrace();
                 }

        }

        return r1;

	}
               
             
	//This is the interface with the client
	@Override
        public requests requestRead (String fileName, int nodeId){ 
	//System.out.println("\n\nRead requested on node: " + nodeId);	
	requests r1= new requests();
	
	if(nodeId== SNServer.SNId)
	{
		//System.out.println("Write on coordinator node");
		r1=coordinatorFunc(fileName, 0, "");
	}
	else
	{
		//RPC call to coordinator
	try {
	      	TTransport transport;

      		//Taking the ip and port number of the nodes in the list of activeNodes
      		transport = new TSocket(SNServer.SNIp, SNServer.SNPort);

      		TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
		NService.Client client = new NService.Client(protocol);

	        transport.open();

		//System.out.println("Calling coordinator fn from this node");

	        r1= client.coordinatorFunc(fileName, 0, "");

		//System.out.println("Back from cordinatorfn");

	        transport.close();
	      } catch (TException x) {
	          x.printStackTrace();
   		 }

	}


	return r1;
	}


        //The coordinator function
        @Override
	public  requests coordinatorFunc(String fileName,int  op, String content){
	//System.out.println("Inside coor fn");
	requests r1= new requests();
	
	if(op==0){

	System.out.println("Requesting a read lock");
        SNServer.lock.readLock().lock();
        System.out.println("Acquired a read lock");


	LinkedList<node> readList= formQuorumList(0);
	r1= readNewestVersion(fileName, readList);

	SNServer.lock.readLock().unlock();
        System.out.println("Read lock released");
        System.out.println("---------------------------------------------------");
	System.out.println("Read operation completed!")	;
	System.out.println("---------------------------------------------------");


	return r1;
	}

	else{

	System.out.println("Waiting to get a write lock");
	SNServer.lock.writeLock().lock();
	System.out.println("Acquired a write lock");

	LinkedList<node> writeList = formQuorumList(1);
	r1= getHighestVersion(fileName, writeList, content);

	SNServer.lock.writeLock().unlock();
        System.out.println("Released the write lock");
        System.out.println("---------------------------------------------------");
        System.out.println("Write operation completed!") ;
        System.out.println("---------------------------------------------------");



	return r1;
	}

	}

        @Override
	public LinkedList<node> formQuorumList(int op){
	LinkedList<node> temp = new LinkedList<node>();
	node tnode= new node();
	//System.out.println("Inside Form quorum");
	int t= (op == 0? SNServer.nr: SNServer.nw); 
	//System.out.println("Size of t: "+t);
		while(temp.size()!= t)
		{
			//System.out.println("t now is:"+t);
			tnode= giveRandomNode();
			if(!temp.contains(tnode))
				temp.add(tnode);
			
		}

	System.out.print("Nodes in quorum for this op: "+ op + " are: ");
	for(node n:temp){
	System.out.print(n.id + " ");
        }
	System.out.println();
	return temp;	
	}
	

	//This function takes the highest version of the file from the nodes in write quorum
	//Increases that version by 1 and performs the write operation with new content
	@Override
        public  requests getHighestVersion (String fileName, List<node> writeList, String content){
			
	//System.out.println("Inside getHighestVersion of Write call");
        requests r= new requests();
        int maxVersion= -1, tempVersion=-1;
        //node maxNode= new node();
        for(node n: writeList) {
                //tempVersion=RPC call and take version
                try {
                      TTransport transport;
                      //Taking the ip and port number of the nodes in the list of activeNodes
                      transport = new TSocket(n.ip, n.port);
                      TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
                      NService.Client client = new NService.Client(protocol);
                      transport.open();
		      //System.out.println("Calling get version of"+ n.id);
                      tempVersion=client.getFileVersion(fileName);
                      //System.out.println("tempVers=" + tempVersion);

                      transport.close();
                    } catch (TException x) {
                        x.printStackTrace();
                    }
                if(tempVersion> maxVersion){
                        maxVersion= tempVersion;
                        //maxNode= n;
                }
        }

	//System.out.println("Max Version for " + fileName + "in Nw is" + maxVersion);
        //if Maxversion=-1, then store file as file.0 else maxversion=x, as file.x+1
        maxVersion++;

	//Populate the maps
	List<node> tempList= new LinkedList<node>(SNServer.nodeList);
	tempList.removeAll(writeList);
	SNServer.eventual1.put(fileName,tempList);
	SNServer.eventual2.put(fileName,new Integer(maxVersion));
	SNServer.eventual3.put(fileName,content);

           
        //perform write on each node in write list of quorum
	for(node n : writeList){ 
        	try {
              		TTransport transport;
             		 //Taking the ip and port number of the nodes in the list of activeNodes
              		transport = new TSocket(n.ip, n.port);
              		TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
              		NService.Client client = new NService.Client(protocol);
              		transport.open();
              		//Writing on the nodes
              		System.out.println("Calling doWrite on "+ n.id +"for filename:  "+  fileName+"."+maxVersion);
              		r.status= client.doWrite(fileName+"."+ maxVersion, content);
			r.contents="";

              transport.close();
            } catch (TException x) {
              x.printStackTrace();
            }
	}
	 return r;
	}



 	@Override
        public requests readNewestVersion(String fileName, List<node> readList){
	
	//System.out.println("Inside read newest version");
	requests r= new requests();
	int maxVersion= -1, tempVersion=-1;
	node maxNode= new node();
	for(node n: readList)	
	{
		//Calling all nodes in read quorum to get the max version of the file
		try {
		      TTransport transport;
		      //Taking the ip and port number of the nodes in the list of activeNodes
		      transport = new TSocket(n.ip, n.port);
		      TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
		      NService.Client client = new NService.Client(protocol);
		      transport.open();
			//System.out.println("Calling get version of the node"+ n.id);
		      tempVersion=client.getFileVersion(fileName);
			//System.out.println("tempVers=" + tempVersion);
		      transport.close();
		    } catch (TException x) {
		        x.printStackTrace();
    		      }

		if(tempVersion> maxVersion){
			maxVersion= tempVersion;
			maxNode= n;
		}
	}     
	//System.out.println("Maxversion for file" + fileName + "is" + maxVersion );


	//Requested file is not present on any if the nodes in Read Quorum
        if(maxVersion==-1)
	{
		r.contents="";
		r.status= 0;
		return r;
	}
		

	System.out.println("Latest copy of requested file is present at node "+ maxNode.id);
	//Requested file is present on one of the nodes so reading from the latest version
	try {
	      TTransport transport;

	      //Taking the ip and port number of the nodes in the list of activeNodes
	      transport = new TSocket(maxNode.ip, maxNode.port);

	      TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
	      NService.Client client = new NService.Client(protocol);

	      transport.open();
		
	      //Reading the latest file from the node
	
	      //System.out.println("Calling do read on node "+ maxNode.id +"for filename:  "+  fileName+"."+maxVersion);
	      r.contents= client.doRead(fileName+"."+ maxVersion);
	      r.status=1;

	      transport.close();
	    } catch (TException x) {
	      x.printStackTrace();
	    }

	      return r;
		
	}

        @Override        
	public int  getFileVersion(String fileName){
	//System.out.println("Going to find the version in scratch "+ fileName);
	File myFolder= new File("/export/scratch/kajar_mahal");
	File[] listOfFiles = myFolder.listFiles();
	//System.out.println("list of files"+ listOfFiles.length);
	String myFileName="";
	//System.out.println("Inside getversion of node"+ NodeServer.myNode.id);
	for(int i=0; i< listOfFiles.length; i++)
	{
		if(listOfFiles[i].isFile())
		{
			myFileName= listOfFiles[i].getName();
			//System.out.println("myFileName insideloop ofget version:" + myFileName);
			if(myFileName.substring(0, myFileName.indexOf(".")).equals(fileName))	{
					//System.out.println("Foundvers:"+Integer.parseInt(myFileName.substring(myFileName.indexOf(".")+1)));
					return  Integer.parseInt(myFileName.substring(myFileName.indexOf(".")+1));
				}	
		}
			
	}
	//System.out.println("File is not there");
	return -1;
	}
	
    
   	@Override
	public String doRead(String fileName){
	//System.out.println("Looking for file named: "+ fileName);
	StringBuffer content= new StringBuffer("");
	try{
	for (String line : Files.readAllLines(Paths.get("/export/scratch/kajar_mahal", fileName), Charset.forName("US-ASCII"))) 
    		content.append(line);	   		
    	}catch(IOException e){
	e.printStackTrace();
	}

	return content.toString();
	}

        @Override        
	public int doWrite(String fileName, String content){
	String pathToScan = "/export/scratch/kajar_mahal";
        String target_file ;  // fileThatYouWantToFilter
        File folderToScan = new File(pathToScan); 
    	File[] listOfFiles = folderToScan.listFiles();
	int flag=0;
	List<String> myList= new LinkedList<String>();
     	for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                target_file = listOfFiles[i].getName();
		
                if (target_file.startsWith(fileName.substring(0, fileName.indexOf(".")+1))){
			File f = new File("/export/scratch/kajar_mahal/"+target_file);
			try{
			FileOutputStream fStream = new FileOutputStream(f, false); 
                  	byte[] myBytes = content.getBytes(); 
			fStream.write(myBytes);
			f.renameTo(new File("/export/scratch/kajar_mahal/" + fileName));
			fStream.close();
			}
			catch(Exception e){
			e.printStackTrace();
			}

	                System.out.println(fileName+ " is written successfully to this node");
			flag=1;
			
                 }
                }
         }

	//No previous version of the file is found
	if(flag==0){
	try{
        	File f= new File("/export/scratch/kajar_mahal/"+ fileName);
       		FileWriter fwrite;
                f.createNewFile();
        	fwrite= new FileWriter(f.getAbsoluteFile());
        	BufferedWriter bufwrite= new BufferedWriter(fwrite);
        	bufwrite.write(content);
		bufwrite.close();
        	System.out.println(fileName+ " is written successfully to this node");
        	flag=1;
       	}
        catch(IOException e){
                e.printStackTrace();
        }
	}
	
	//Display the list of files after the operation
	myList= getFilesOnThisNode();
	//System.out.println("-----------------------------------");
	System.out.println("List of files on this node:");
	for(String s: myList)
		System.out.print(s+" ");
	System.out.println();
	System.out.println("-----------------------------------");

        return flag;
        
	}

	@Override
        public int getId (node newnode) throws TException {
	//System.out.println("Its here and nodelist size is "+ SNServer.nodeList.size());
        int tempid= getNewId();
        while(searchId(tempid)){

                tempid= getNewId();
        }
        //System.out.println("After searching");
        newnode.id= tempid;
        //LinkedList<node> templist= nodeList;
        SNServer.nodeList.add(newnode);
        //return templist;
        //System.out.println("NewNode id:"+ newnode.id);
        return newnode.id;
        }


	//Searching the generated ID in the nodeList to avoid duplication of ID
        public boolean searchId(int tempid){

        if(SNServer.nodeList==null)
                return false;

        for(node n : SNServer.nodeList){

                if(n.id==tempid)
                        return true;
        }

                return false;
        }

        //Getting a random ID between 0 and 31
        public int getNewId ()
        {
                Random r = new Random();
                int low = 1;
                int high = SNServer.nTotal;

                return (r.nextInt((high-low)+1) + low);

        }


	@Override
	public List<String> getFilesOnThisNode(){
        	//System.out.println("Going to find the version in scratch "+ fileName);
	        File myFolder= new File("/export/scratch/kajar_mahal/");
        	File[] listOfFiles = myFolder.listFiles();
	        List<String> myList = new LinkedList<String>();
	        String myFileName="";
	        //System.out.println("List of files on this node are:");
	        for(int i=0; i< listOfFiles.length; i++)
	        {
	                if(listOfFiles[i].isFile() && !listOfFiles[i].getName().equals("README.txt"))
        	        {
                	        myList.add(listOfFiles[i].getName());
                        	//System.out.println(listOfFiles[i].getName());
	                }
        	}

	        return myList;

        }
	
	public Map<Integer, List<String>> getFilesOnSystem(){

	Map<Integer, List<String>> fileState= new HashMap<Integer, List<String>>();	
	for(node n: SNServer.nodeList){

		try {
                      TTransport transport;
                      //Taking the ip and port number of the nodes in the list of activeNodes
                      transport = new TSocket(n.ip, n.port);
                      TProtocol protocol = new  TBinaryProtocol(new TFramedTransport(transport));
                      NService.Client client = new NService.Client(protocol);
                      transport.open();
                      fileState.put(new Integer(n.id), client.getFilesOnThisNode());
                      transport.close();
                    } catch (TException x) {
                        x.printStackTrace();
                      }

	}

	return fileState;

	}
}
