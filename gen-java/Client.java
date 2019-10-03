import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.*;
import java.io.*;


/**
  Class: Ipport
  Provides a structure that storees ip and port details
**/

class Ipport{
  String ip;
  int port;
  Ipport(String ip, int port){
      this.ip = ip;
      this.port = port;
  }
}

/**
  Class Client
  1. A file is provided which contains the list of file servers and list of operations
  2. The files servers are parsed and stored in an array listOfFiles
  3. Client parses the operations (read/write) and sends them to one of the servers randomly
  4. Client displays the total run-time for the associate file
**/

public class Client {
    static List<String> listOfFiles = new ArrayList<>();
    public static ArrayList<Ipport> arrList;
    public static int totalServers = 0;

    public static void main(String [] args) {
        arrList = new ArrayList<>();

        try{
          BufferedReader br = new BufferedReader(new FileReader("./TestFiles/"+args[0]));
          String st;
          while ((st = br.readLine()) != null){
            String[] strSplit = st.split(" ");
            if (!strSplit[0].equals("r") || !strSplit[0].equals("w")){
                Ipport p = new Ipport(strSplit[0], Integer.valueOf(strSplit[1]));
                arrList.add(p);
                ++totalServers;
            }
          }
        }
        catch(Exception e){}

        String ret = "";
        long sTime = System.currentTimeMillis();
        try{
          BufferedReader br = new BufferedReader(new FileReader("./TestFiles/"+args[0]));
          String st;
          while ((st = br.readLine()) != null){
            String[] strSplit = st.split(" ");
            if (strSplit[0].equals("r") || strSplit[0].equals("w")) {
              try {
                    String rw = strSplit[0];
                    String fileName = strSplit[1];

                    Random rand = new Random();
                    int n = rand.nextInt(totalServers);
                    Ipport randomIpport = arrList.get(n);
                    String ip = randomIpport.ip;
                    int port = randomIpport.port;

                    TTransport  transport = new TSocket(ip, port);
                    TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                    NodeService.Client client = new NodeService.Client(protocol);
                    transport.open();
                    client.ping();
                    if(rw.equals("r")){
                         ret = client.readFromServer(fileName, totalServers);
                    }
                    else if(rw.equals("w")){
                        ret = client.writeToServer(fileName, totalServers);
                    }
                    else{
                        System.out.println("Incorrect Values in File");
                    }
                    transport.close();
                    System.out.println(ret);
                    if(ret.equals("The request could not be completed at the moment")) break;
                    if(ret.equals("Total Number of Servers in Test File not equal to total number of connected servers")) break;
                }
                catch(TException e){
                  System.out.println("Exception in Reading/Writing from Server or,");
                  System.out.println("The number of running servers are not equal to the number of servers in test file ");
                  break;
                }
            }
            else{
                continue;
            }
          }
        }
        catch(Exception e){System.out.println("Exception in Client: "+e);}
        long eTime = System.currentTimeMillis();
        long execTime = eTime - sTime;
        System.out.println("Total Execution Time For File "+args[0]+" = "+execTime+" ms");
      }
}
