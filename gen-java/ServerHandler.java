import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.TException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
  Class: ServerHandler : Handler for the Server Nodes (including coordinator node)
**/

public class ServerHandler implements NodeService.Iface
{
        static List<String> serverList = new ArrayList<>();
        static String ip, coordinatorIp;
        static int port, coordinatorPort;
        static int uniqueId = 0;
        static int totalServers = 0;
        static int pendingReads =0;
        static Map<String, Integer> versionMap;
        static ServerHandler sHandler;
        static ConcurrentLinkedQueue<String> coordinationQueue = new ConcurrentLinkedQueue<String>();
        static ConcurrentHashMap<String,String> concurrentHashMap = new ConcurrentHashMap<String,String>();
        static Set<String> currentWrites = concurrentHashMap.newKeySet();

        @Override
        public boolean ping() throws TException {
			       return true;
		    }

        /*
           readFromServer():
           1. Accepts a read request from the client
           2. Assembles a quorum list and finds the server with the highest version of the file
           3. Fetches the file contents form the server with highest version no
           4. Returns contents back to the client or negative response
        */
        @Override
        public String readFromServer (String fileName, int passedServers) throws TException{
              List<String> quorumList = new ArrayList<>();
              String retString="";

              try {
                   TTransport  transport = new TSocket(coordinatorIp, coordinatorPort);
                   TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                   NodeService.Client client = new NodeService.Client(protocol);
                   transport.open();
                   quorumList = client.requestQuorum("r",ip, port, fileName, passedServers);
                   transport.close();
              } catch(Exception e) {e.printStackTrace();}

            if(quorumList.isEmpty()) return "The request could not be completed at the moment";

             String tempIp=""; int tempPort = 0; int maxVersion = 0;
             for(String s : quorumList){
                 String[] strSplit = s.split(":");
                 try {
                   TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                   TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                   NodeService.Client client = new NodeService.Client(protocol);
                   transport.open();
                   int retVersion = client.getVersionNumber(fileName);
                   if(retVersion>maxVersion){
                       tempIp = strSplit[0]; tempPort = Integer.valueOf(strSplit[1]);
                       maxVersion = retVersion;
                   }
                   transport.close();
                 } catch(TException e) {}
             }
             if(maxVersion == 0){
                retString = fileName+" does Not Exist in the System";
             }
             else{
                 try {
                   TTransport transport = new TSocket(tempIp, tempPort);
                   TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                   NodeService.Client client = new NodeService.Client(protocol);
                   transport.open();
                   retString = client.readFromFileSystem(fileName);
                   retString = "Read Complete => "+retString;
                   transport.close();
                 } catch(TException e) {System.out.println("Exception in readFromFileSystem(): "+e);}
             }
             try {
                   TTransport  transport = new TSocket(coordinatorIp, coordinatorPort);
                   TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                   NodeService.Client client = new NodeService.Client(protocol);
                   transport.open();
                   client.readComplete();
                   transport.close();
               } catch(TException e) {System.out.println("Exception in readComplete(): "+e);}

             return retString;
        }

        /*
           writeToServer():
           1. Accepts a write request from the client
           2. Assembles a quorum list and finds the server with the highest version of the file
           3. Writes the new file contents on the servers with highest version no for the file
           4. Returns an acknowledgment to the client
        */

        @Override
        public String writeToServer(String fileName, int passedServers) throws TException{
           List<String> quorumList = new ArrayList<>();

           try {
                TTransport  transport = new TSocket(coordinatorIp, coordinatorPort);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client client = new NodeService.Client(protocol);
                transport.open();
                quorumList = client.requestQuorum("w",ip, port,fileName, passedServers);
                transport.close();
           } catch(Exception e) {e.printStackTrace();}

          if(quorumList.isEmpty()) return "The request could not be completed at the moment";

          String tempIp=""; int tempPort = 0; int maxVersion = 0;
          for(String s : quorumList){
              String[] strSplit = s.split(":");
              try {
                TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client client = new NodeService.Client(protocol);
                transport.open();
          			int retVersion = client.getVersionNumber(fileName);
                if(retVersion>maxVersion){
                    tempIp = strSplit[0]; tempPort = Integer.valueOf(strSplit[1]);
                    maxVersion = retVersion;
                }
                transport.close();
              } catch(TException e) {}
          }
          if(maxVersion == 0){
             versionMap.put(fileName, 1);
             try{
               sHandler.writeToFile(fileName);
             }
             catch(Exception e){System.out.println("Exception in writeToFile(): "+e);}
          }
          else{
              try {
                TTransport transport = new TSocket(tempIp, tempPort);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client client = new NodeService.Client(protocol);
                transport.open();
                client.writeToFileSystem(fileName);
                transport.close();
              } catch(TException e) {System.out.println("Exception in writeToFileSystem(): "+e);}
          }

          //call to coordinator
          try {
                TTransport  transport = new TSocket(coordinatorIp, coordinatorPort);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client client = new NodeService.Client(protocol);
                transport.open();
                client.writeComplete(fileName);
                transport.close();
            } catch(TException e) {System.out.println("Exception in Write Complete(): "+e);}

          return "Write Complete on "+fileName;
        }


        /*
           joinCoordinator():
           Helps a new node to update its presence to the coordinator
        */
        @Override
        public boolean joinCoordinator(String coordinatorIp, int coordinatorPort, String ip, int port) throws TException{
          versionMap = new HashMap<>();
          sHandler = new ServerHandler();
          this.coordinatorIp = coordinatorIp;
          this.coordinatorPort = coordinatorPort;
          this.ip = ip;
          this.port = port;
          System.out.println("Joined Server: "+this.ip+" : "+this.port);
          try {
                TTransport  transport = new TSocket(coordinatorIp, coordinatorPort);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                NodeService.Client client = new NodeService.Client(protocol);
                transport.open();
                client.updateCoordinatorList(ip, port);
                transport.close();
              } catch(TException e) {System.out.println("Exception in joinCoordinator(): "+e);}
              return true;
        }

        /*
           updateCoordinatorList():
           Coordinator registers the details of the new node
        */
        @Override
        public void updateCoordinatorList(String ip, int port) throws TException{
            System.out.println("Servers Added To Co-ordinator:");
            serverList.add(ip+":"+port);
            ++totalServers;
            System.out.println("Total Server: "+totalServers);
            for(String s : serverList){
                System.out.println(s);
            }
        }

        /*
           intializeCoordinator():
           Sets up the coordinator, when it joins the distributed file system
        */
        public void intializeCoordinator(String coordinatorIp, int coordinatorPort) throws TException{
            sHandler = new ServerHandler();
            this.coordinatorIp = coordinatorIp;
            this.coordinatorPort = coordinatorPort;
            this.ip = coordinatorIp;
            this.port = coordinatorPort;
            serverList.add(coordinatorIp+":"+coordinatorPort);
            ++totalServers;
            versionMap = new HashMap<>();
            for(String s : serverList){
                System.out.println(s);
            }
        }

        /*
           requestQuorum():
           Randomly generates a quorum and returns a list
        */
        @Override
        public List<String> requestQuorum(String type, String ip, int port, String fileName, int passedServers) throws TException{
          List<String> quorumList = new ArrayList<String>();
          String uniqueKey="";
          try{
            if(passedServers != totalServers) {
                System.out.println("Total Number of Servers in Test File not equal to total number of connected servers");
                throw new TException();
            }
          }
          catch(TException e){
              return quorumList;
          }

          synchronized(this){
              ++ uniqueId;
              uniqueKey = type+"_"+uniqueId+"_"+ip+"_"+port;
          }
          coordinationQueue.add(uniqueKey);
          while(!coordinationQueue.peek().equals(uniqueKey)){
              Thread.yield();
          }
          String[] strSplit = uniqueKey.split("_");
          Random rand = new Random();
          int nw=0;
          int nr=0;
          int qsize=0;

          try{
            BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
            String st;
            st = br.readLine();
            String[] configSplit = st.split(" ");
            nr = Integer.valueOf(configSplit[0]);
            nw = Integer.valueOf(configSplit[1]);
            if(nw<=(totalServers/2)) {
                System.out.println("nw should be greater than (totalServers/2): check in config.txt");
                throw new TException("nw");
            }
            if(nr+nw<=totalServers) {
                System.out.println("nr+nw should be greater than totalServers: check in config.txt");
                throw new TException("nr");
            }
          }
          catch(Exception e){
            return quorumList;
          }

          if(strSplit[0].equals("w")){
              while(pendingReads!=0 || currentWrites.contains(fileName)){
                Thread.yield();
              }
              currentWrites.add(fileName);
              coordinationQueue.remove();
              qsize=nw;
          }
          else{
              synchronized(this){
                pendingReads++;
              }
              coordinationQueue.remove();
              qsize=nr;
          }
          while(qsize > 0){
            int n = rand.nextInt(totalServers);
            String quorumEntity = serverList.get(n);
            if(quorumList.contains(quorumEntity)) continue;
            quorumList.add(quorumEntity);
            qsize--;
          }
          return quorumList;
        }

        /*
           getVersionNumber():
           Returns a version number for a given file
           Returns -1 iff FileNotFound
        */
        @Override
        public int getVersionNumber(String fileName) throws TException{
          return versionMap.get(fileName)==null?-1:versionMap.get(fileName);
        }

        /*
           writeComplete():
           In case the write operation is completed, it polls the entry
           from the coordinator queue so that the execution can proceed
        */
        @Override
        public void writeComplete(String fileName) throws TException{
            //coordinationQueue.poll();
            currentWrites.remove(fileName);
        }

        /*
           writeToFileSystem():
           Updates the new file version
           Calls writeToFile()
        */
        @Override
        public boolean writeToFileSystem(String fileName) throws TException{
            versionMap.put(fileName, versionMap.get(fileName)+1);
            try{
              sHandler.writeToFile(fileName);
            }
            catch(IOException e){System.out.println("Error in writeToFileSystem");}
            return true;
        }

        /*
           readComplete():
           In case the read operation is completed, it decreases pendingReads
        */
        @Override
        public void readComplete() throws TException{
            synchronized(this){
              pendingReads--;
            }
        }

        /*
           readFromFileSystem():
           Reads the requested file contents and returns it to the caller
        */
        @Override
        public String readFromFileSystem(String fileName) throws TException{
          File file = new File("./"+sHandler.ip+sHandler.port+"/"+fileName);
          BufferedReader br;
          StringBuilder sb = new StringBuilder();
          try {
              br = new BufferedReader(new FileReader(file));
              String line = br.readLine();
              sb.append(line);
              br.close();
          }
          catch (Exception e){System.out.println("File Not Available / Can not be Read At the Moment");}
          return sb.toString();
        }


        @Override
        public Map<String,Integer> synchGet() throws TException{
            return versionMap;
        }

        /*
           synchPut():
           The synchronized file data sent from the coordinator is written on the server
        */
        @Override
        public void synchPut(Map<String,Integer> updatedVersions) throws TException{
            versionMap.putAll(updatedVersions);
            for(String key : versionMap.keySet()){
                System.out.println(key+"  "+versionMap.get(key));
                try {
                      File file = new File("./"+ip+port);
                      if(!file.exists()) file.mkdir();
                      file = new File("./"+ip+port+"/"+key);
                      FileWriter fileWriter = new FileWriter(file);
                      fileWriter.write(key+" Version Number:"+versionMap.get(key));
                      fileWriter.flush();
                      fileWriter.close();
                } catch (IOException e) {e.printStackTrace();}
            }
            System.out.println("Synch Complete\n");
        }


        public void writeToFile(String fileName) throws IOException {
          try {
              File file = new File("./"+sHandler.ip+sHandler.port);
              file.mkdir();
              file = new File("./"+sHandler.ip+sHandler.port+"/"+fileName);
              FileWriter fileWriter = new FileWriter(file);
              fileWriter.write(fileName+" Version Number:"+versionMap.get(fileName));
              fileWriter.flush();
              fileWriter.close();
            } catch (IOException e) {e.printStackTrace();}
        }

        /*
           synch():
           1. Runs on the coordinator server and is called every 10 secs
           2. Gets the latest file versions from all the Servers
           3. Writes the latest file contents and version to all the Servers
           4. Helps realize eventual consistency
        */
        public static void synch(){
            for(String s : serverList){
                Map<String, Integer> tempMap = new HashMap<>();
                String[] strSplit = s.split(":");
                if(!strSplit[0].equals(ip) || Integer.valueOf(strSplit[1]) != port) {
                  try {
                        TTransport  transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                        NodeService.Client client = new NodeService.Client(protocol);
                        transport.open();
                        tempMap = client.synchGet();
                        transport.close();
                    } catch(TException e) {System.out.println("Server is busy serving other request");}
                    //code to update versionMap
                    for(String key : tempMap.keySet()){
                        if(versionMap.containsKey(key)){
                            versionMap.put(key, Math.max(tempMap.get(key), versionMap.get(key)));
                        }
                        else{
                            versionMap.put(key, tempMap.get(key));
                        }
                        try {
                              File file = new File("./"+sHandler.ip+sHandler.port);
                              if(!file.exists()) file.mkdir();
                              file = new File("./"+sHandler.ip+sHandler.port+"/"+key);
                              FileWriter fileWriter = new FileWriter(file);
                              fileWriter.write(key+" Version Number:"+versionMap.get(key));
                              fileWriter.flush();
                              fileWriter.close();
                        } catch (IOException e) {e.printStackTrace();}
                    }
                }
            }

            for(String s : serverList){
                String[] strSplit = s.split(":");
                if(!strSplit[0].equals(ip) || Integer.valueOf(strSplit[1]) != port) {
                  try {
                        TTransport  transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                        NodeService.Client client = new NodeService.Client(protocol);
                        transport.open();
                        client.synchPut(versionMap);
                        transport.close();
                    } catch(TException e) {System.out.println("Server is busy serving other requests");}
                }
            }
        }

}
