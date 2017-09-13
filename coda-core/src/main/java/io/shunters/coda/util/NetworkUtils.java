package io.shunters.coda.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mykidong on 2016-05-25.
 */
public class NetworkUtils {
    private static Set<String> PRIVATE_IP_SET = new HashSet<String>();

    static {
        PRIVATE_IP_SET.add("192");
        PRIVATE_IP_SET.add("172");
    }

    public static boolean isValidIp(String ip) {
        boolean isValid = false;
        String[] tmp = null;

        if ((ip != null) && ((tmp = ip.split("[.]")) != null)
                && (tmp.length == 4)
                && (PRIVATE_IP_SET.contains(tmp[0]) == false)) {
            isValid = true;
        }

        return isValid;
    }

    public static InetAddress getInetAddress() throws Exception {
        for (Enumeration<NetworkInterface> en = NetworkInterface
                .getNetworkInterfaces(); en.hasMoreElements();) {
            NetworkInterface intf = en.nextElement();

            for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr
                    .hasMoreElements();) {
                InetAddress inetAddress = enumIpAddr.nextElement();

                if (inetAddress instanceof Inet4Address
                        && !inetAddress.isLoopbackAddress() // localhost 제외
                        && !inetAddress.isLinkLocalAddress()
                        && isValidIp(inetAddress.getHostAddress())) {
                    return inetAddress;
                }
            }
        }

        return InetAddress.getLocalHost();
    }

    public static String getHostIp() {
        String ip = null;

        try {
            InetAddress addr = getInetAddress();
            ip = addr.getHostAddress();
        } catch (Exception e) {

        }

        return ip;
    }

    public static String getHostName() {
        String hostname = null;

        try {
            InetAddress addr = getInetAddress();
            hostname = addr.getHostName();
        } catch (Exception e) {

        }

        return hostname;
    }
}
