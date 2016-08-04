package com.zj.zookeeper.other;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jufeng on 16/8/4.
 *
 */
public class ConfigWatcher implements Watcher{



    private ZooKeeper zooKeeper ;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final int TIMEOUT = 5000;
    private static final String servers = "127.0.0.1:2181";


    public ConfigWatcher() {
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

    public void setData(String path,byte [] data){
        try {
            Stat stat = zooKeeper.exists(path,false);
            if (stat==null){
                zooKeeper.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
                System.out.println("新建 "+path+" 节点,写入数据");
            }else {
                zooKeeper.setData(path,data,-1);
                System.out.println(""+path+" 节点,写入数据");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getData(String path){

        try {
            byte[] data =  zooKeeper.getData(path,this,null);
            return  new String (data);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void close(){
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged){
            System.out.println("数据被重写了......");
        }
    }


    public static void main(String [] args){
        ConfigWatcher c = new ConfigWatcher();
        c.setData("/test001","im ajun".getBytes());
        c.getData("/test001");//开始监听
        c.setData("/test001","im ajun 001".getBytes());
        c.close();

    }
}
