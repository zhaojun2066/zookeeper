package com.zj.zookeeper.other;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jufeng on 16/8/4.
 * 利用zk 实现单点 最小节点为master 启动
 *
 * 创建临时有序节点,大节点加监听比他本身小的且是最大的节点变化（NodeDelete事件）
 *
 * 多次执行main 方法 每次修改serer name
 * 然后停掉master 看其他slave 变化,是否有升为master 的slave server
 * 实验证明成功
 */
public class SinglePoint  implements Watcher{

    private ZooKeeper zooKeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final int TIMEOUT = 80000;
    private static final String servers = "127.0.0.1:2181";
    private String  serverName = "";
    private static  final String ROOT = "/root004";

    //private ExecutorService services = Executors.newFixedThreadPool(10);


    public SinglePoint(String serverName) {
        this.serverName = serverName;
        try {
            zooKeeper = new ZooKeeper(servers, TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState().SyncConnected == Event.KeeperState.SyncConnected){
                        countDownLatch.countDown();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createPath(String path,byte [] data){
        try {
            Stat stat = zooKeeper.exists(path,false);
            if (stat==null){
                zooKeeper.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }else {
                zooKeeper.setData(path,data,-1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void createRoot(){
        this.createPath(ROOT,null);
        System.out.println("创建root节点成功...........");
    }

    public void createSeq(){
        //可以存储server 节点信息,测试就不存储了
        try {
            zooKeeper.create(ROOT+"/servers", serverName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("创建子节点成功...........");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
      //  this.createPath(ROOT+"/servers",serverName.getBytes(),CreateMode.EPHEMERAL_SEQUENTIAL);

    }

    public String getData(String path){
        String s = null;
        try {
            byte [] data =this.zooKeeper.getData(path,false,null);
            if (data!=null){
                s= new String(data);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(path+"..........." +s);
        return  s;
    }


    /**
     * 检查谁是最后的master ,且看设置各自的watcher
     */
    public void check(){
        try {
            List<String> servers =  this.zooKeeper.getChildren(ROOT,false);
            if(servers==null || servers.isEmpty()){
                System.out.println("servers is empty....");
            }else {
                //排序
                Collections.sort(servers);
                String master = servers.get(0);
                String mName = this.getData(ROOT+"/"+master);
                if (mName.equals(serverName)){
                    this.startMaster();
                }else {
                    int size = servers.size();
                    if(size==1){
                        startMaster();
                        return;
                    }
                    //后一个节点监听前一个变化
                    String next =null;

                    for (int j = 0; j < size; j++) {
                        String selfName = this.getData(ROOT+"/"+servers.get(j));
                        int preIndex = j-1;
                        if (serverName.equals(selfName) && preIndex<size){
                            next = servers.get(preIndex);
                            break;
                        }
                    }

                    //说明此节点有要监听的节点
                    if (next!=null && !"".equals(next)){
                        zooKeeper.exists(ROOT+"/"+next,this);
                        System.out.print("monitor: "+ROOT+"/"+next);
                    }


                }


            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void close(){
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startMaster(){
        System.out.println(serverName+ " start....");
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeDeleted){
            System.out.println("has node down,chenge master");
            check();
        }
    }



    /*static class Server implements Runnable{

        private SinglePoint sp;

        public Server(SinglePoint sp) {
            this.sp = sp;
        }

        @Override
        public void run() {
            sp.createSeq();
            sp.check();
        }
    }*/



    public static void main(String [] args){
        SinglePoint sp = new SinglePoint("127.0.0.1:5000");
        //sp.createRoot();
        sp.createSeq();
        sp.check();
        System.out.println("Server : "+ sp.serverName);
        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sp.close();
    }
}
