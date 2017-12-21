package org.apache.flume.clients.log4jappender;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Properties;

/**
 * Created by tick on 2017/11/21.
 */
public class FailoverLog4jAppender extends Log4jAppender {

    private String hosts;
    private String maxAttempts;
    private String connectTimeout;
    private String requestTimeout;
    private boolean configured = false;

    public void setHosts(String hostNames) {
        this.hosts = hostNames;
    }

    public void setMaxAttempts(String maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public void setConnectTimeout(String connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public void setRequestTimeout(String requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    @Override
    public synchronized void append(LoggingEvent event) {
        if (!configured) {
            String errorMsg = "Flume Log4jAppender not configured correctly! Cannot" +
                    " send events to Flume.";
            LogLog.error(errorMsg);
            if (getUnsafeMode()) {
                return;
            }
            throw new FlumeException(errorMsg);
        }
        super.append(event);
    }

    /**
     * Activate the options set using <tt>setHosts()</tt>
     *
     * @throws FlumeException if the LoadBalancingRpcClient cannot be instantiated.
     */
    @Override
    public void activateOptions() throws FlumeException {
        try {
            final Properties properties = getProperties(hosts, maxAttempts,
                    connectTimeout, requestTimeout);
            rpcClient = RpcClientFactory.getInstance(properties);
            if (layout != null) {
                layout.activateOptions();
            }
            configured = true;
        } catch (Exception e) {
            String errormsg = "RPC client creation failed! " + e.getMessage();
            LogLog.error(errormsg);
            if (getUnsafeMode()) {
                return;
            }
            throw new FlumeException(e);
        }

    }

    private Properties getProperties(String hosts, String maxAttempts,
                                     String connectTimeout, String requestTimeout)
            throws FlumeException {
        if (StringUtils.isEmpty(hosts)) {
            throw new FlumeException("hosts must not be null");
        }

        Properties props = new Properties();

        String[] hostsAndPorts = hosts.split("\\s+");
        StringBuilder names = new StringBuilder();
        for (int i = 0; i < hostsAndPorts.length; i++) {
            String hostAndPort = hostsAndPorts[i];
            String name = "h" + i;
            props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + name,
                    hostAndPort);
            names.append(name).append(" ");
        }
        props.put(RpcClientConfigurationConstants.CONFIG_HOSTS, names.toString());
        props.put(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
                RpcClientFactory.ClientType.DEFAULT_FAILOVER.toString());

        props.setProperty(RpcClientConfigurationConstants.CONFIG_MAX_ATTEMPTS, maxAttempts);

        props.setProperty(RpcClientConfigurationConstants.CONFIG_CONNECT_TIMEOUT, connectTimeout);
        props.setProperty(RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT, requestTimeout);

        return props;
    }

}
