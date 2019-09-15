import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.commons.lang.SerializationUtils;

import java.util.*;  
public class Zinspection {
    
    private Boolean full = false ;
    private String hostPort;
    private String clusterName;
    private String tenantName;
    private CuratorFramework client;
    private Map<Integer, String[]> map=new HashMap<Integer,String[]>();
    
    public void intiate() 
    {
        map.put(1, new String[]{"TYPE: ","STATE: "} );
        map.put(2, new String[]{"STATE: ","LEADER NAME: ","DATA: "});
        map.put(3, new String[]{"CAN BE LEADER: ","ACTIVE: "});
    }
    
    public  Zinspection(String hostPort,String clusterName,String tenantName) 
    {
        this.hostPort = hostPort;
        this.clusterName = clusterName;
        this.tenantName = tenantName;
        intiate();
    }
    
    public  Zinspection(String hostPort) 
    {
        this.hostPort = hostPort;
        full = true;
        intiate();
     }
  
    private void mappingParameters(String data , String path) 
    {
        String[] arrOfStr = data.split(":");
        int length = arrOfStr.length  ;
        int key ;
        
        if(path.contains("tasks")) {
            if(length==2)
                key = 3;
            else
                key = 2;
        }
        else
            key = 1;       
        
        String[] parameters =  map.get(key);
        
        for(int i=0;i<length;i++) 
        {
            System.out.print(parameters[i]+arrOfStr[i]+"   ");
        }
    }
    
    public void getInfo(String Path,String space,String name ) 
    {
        try {
               System.out.print(space+name);
               
               List<String> children = client.getChildren().forPath(Path+name);
               byte[] data = client.getData().forPath(Path+name);
               
               if(data.length>0) {
                   System.out.print(space+"---------> ");
                   mappingParameters(new String(data),Path+name);
               }
               
               System.out.println();
               
               if(children.size()==0) 
                   return;
               
               for(String child:children)
               {
                   if(!full) 
                   {
                       if((name.equals("incorta")&&!child.equals(this.clusterName))||(name.equals("tenants")&&!child.equals(this.tenantName)))
                           continue;
                   }
                   getInfo(Path+name+"/",space+"   ",child);
               }
                    
            } catch (Exception e) {
                
                System.out.println("ERROR   "+e);
            }   
    }
    
    public void connect() 
    {
        int sleepMsBetweenRetries = 100;
        int maxRetries = 3;
        RetryPolicy retryPolicy = new RetryNTimes(
          maxRetries, sleepMsBetweenRetries);
         
        client = CuratorFrameworkFactory
          .newClient(hostPort, retryPolicy);
        
        client.start();
    }
    
    public void getTreeModel() 
    {
        getInfo("/","","incorta"); 
    }
    
    public void showDown(String path) 
    {
        try {
            List<String> children = client.getChildren().forPath(path);
            for(String child:children)
            {
                System.out.println(child);
            }
            
        } catch (Exception e) {
            System.out.println(e);
        }
    } 
    
    public void getClusterNames() 
    {
        showDown("/incorta");
    }
    
    public void getClusterTenants(String clusterName) 
    {
        showDown("/incorta/"+clusterName+"/tenants"); 
    }
    
    public static void main(String[] args)
    {
        if(!args[1].equals("get")) {
            System.out.println("Not correct command");
            return;
        }
        
        if(args[2].equals("cluster_tenant")) {
            Zinspection Z= new Zinspection(args[0],args[3],args[4]);
            Z.connect();
            Z.getTreeModel();
            return ;
        }
        
        Zinspection Z= new Zinspection(args[0]);
        Z.connect();
        
        switch(args[2]) {
        case "cluster_names":
            Z.getClusterNames();
            break;
        case "tenant_names":
            Z.getClusterTenants(args[3]);
            break;
        case "all":
            Z.getTreeModel();
            break;
        default:
            System.out.println("Not correct command");
      }
    }    
}
