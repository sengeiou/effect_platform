package com.ali.lz.effect.utils;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * 获取主机IPv4地址和可用空闲端口
 * 
 * @author jiuling.ypf
 * 
 */
public class HostUtil {

    /**
     * 获取本机的IP地址，仅适用于Linux环境下
     * 
     * @return
     */
    public static String getHostIpAddress() {
        Enumeration<NetworkInterface> allNetInterfaces = null;
        try {
            allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                ip = (InetAddress) addresses.nextElement();
                if (ip != null && ip instanceof Inet4Address) {
                    return ip.getHostAddress();
                }
            }
        }
        throw new RuntimeException("failed to get host ip address");
    }

    /**
     * 随机探测并返回1024~16384之间空闲端口
     * 
     * @return
     */
    public static synchronized int getAvaliablePort() {
        final int min = 1024;
        final int max = 16384;
        final int num = max - min + 1;
        final Random rand = new Random();
        InetAddress address;
        try {
            address = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e1) {
            throw new RuntimeException(e1);
        }

        Set<Integer> alreadyTried = new HashSet<Integer>();
        int port = -1;
        while (alreadyTried.size() < num) {
            port = min + rand.nextInt(num);
            if (alreadyTried.contains(port))
                continue;
            try {
                Socket socket = new Socket(address, port);
                alreadyTried.add(port);
                socket.close();
                continue;
            } catch (IOException e) {
                return port;
            }
        }
        throw new RuntimeException("failed to get avaliable port between 1024~16384");
    }

}
