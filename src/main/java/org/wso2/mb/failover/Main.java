package org.wso2.mb.failover;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;

/**
 *
 */
public class Main {
    public static void main(String[] args)
            throws ConfigurationException, JMSException, NamingException,
                   AndesClientConfigurationException, AndesClientException, IOException {
        final Logger log = LoggerFactory.getLogger(Main.class);
        String xmlFilePath = args[0];
        log.info("Config File = " + xmlFilePath);
        XMLConfiguration config = new XMLConfiguration(xmlFilePath);
        AndesClient consumerClient = null;
        if (0 < config.configurationsAt("base.consumer").size()) {
            SubnodeConfiguration consumerNode = config.configurationAt("base.consumer");
            int count = consumerNode.getInt("[@count]", 1);

            log.info("Creating consumer(s)...");
            AndesJMSConsumerClientConfiguration consumerConfig = new
                    AndesJMSConsumerClientConfiguration(xmlFilePath);

            log.info(consumerConfig.toString());

            consumerClient = new AndesClient(consumerConfig, count, true);
            consumerClient.startClient();
        } else {
            log.info("No consumers are created");
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        AndesClient publisherClient = null;
        if (0 < config.configurationsAt("base.publisher").size()) {
            SubnodeConfiguration publisherNode = config.configurationAt("base.publisher");
            int count = publisherNode.getInt("[@count]", 1);

            log.info("Creating publisher(s)...");
            AndesJMSPublisherClientConfiguration publisherConfig = new
                    AndesJMSPublisherClientConfiguration(xmlFilePath);

            log.info(publisherConfig.toString());

            publisherClient = new AndesClient(publisherConfig, count, true);
            publisherClient.startClient();
        } else {
            log.info("No publishers are created");
        }

        log.info("Clients created...");

        final AndesClient finalConsumerClient = consumerClient;
        final AndesClient finalPublisherClient = publisherClient;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (finalPublisherClient != null) {
                        finalPublisherClient.stopClient();
                    }
                    if (finalConsumerClient != null) {
                        finalConsumerClient.stopClient();
                    }

                    AndesClientUtils.flushPrintWriters();
                } catch (JMSException e) {
                    log.error(e.getMessage(), e);
                }
            }
        });

        if (null != consumerClient) {
            AndesClientUtils.waitForMessagesAndShutdown(consumerClient, AndesClientConstants
                                                                            .DEFAULT_RUN_TIME * 15);
        }
    }
}
