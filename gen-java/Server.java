import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.io.*;


/**
  Class: Server
  1. Creates a server for execution
  2. Calls the synch() every 10 secs for the co-ordinator node
**/

public class Server {
    public static ServerHandler handler;
    public static NodeService.Processor processor;

    static String ip, port, coordinatorIp, coordinatorPort;
    static int syncTime=0;
    public static void main(String [] args) {
      ip = args[0];
      port = args[1];
      coordinatorIp = args[2];
      coordinatorPort = args[3];
        try {
            handler = new ServerHandler();
            processor = new NodeService.Processor(handler);

            if(!coordinatorIp.equals("null")) {
                handler.joinCoordinator(coordinatorIp, Integer.valueOf(coordinatorPort), ip, Integer.valueOf(port));
            }
            else{
                handler.intializeCoordinator(ip, Integer.valueOf(port));
                try{
                  BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
                  String st;
                  st = br.readLine();
                  syncTime = Integer.valueOf(br.readLine().trim());
                }catch(Exception e){System.out.println("Exception in syncTime"); e.printStackTrace();}
            }

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };
            new Thread(simple).start();

            Runnable sync = new Runnable() {
                public void run() {
                  while(true && coordinatorIp.equals("null")){
                      handler.synch();
                      System.out.println("Synch Completed");
                      try {Thread.sleep(syncTime);}
                      catch(Exception e) {}
                  }
                }
            };
            new Thread(sync).start();

        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(NodeService.Processor processor) {
        try {
            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(Integer.valueOf(port));
            TTransportFactory factory = new TFramedTransport.Factory();
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
