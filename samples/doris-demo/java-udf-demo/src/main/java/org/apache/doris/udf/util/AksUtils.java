package org.apache.doris.udf.util;

import com.jd.jr.aks.api.AksClient;
import com.jd.jr.aks.api.AksClientFactory;
import com.jd.jr.aks.api.AksHttpClientFactory;
import com.jd.jr.aks.api.model.AksResponse;
import org.apache.doris.udf.aks.Decrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

public class AksUtils {

    private static final Logger logger = LoggerFactory.getLogger(AksUtils.class);
    private static volatile AksClient aksClient;
    private static final Map<String,String> aksConf = new ConcurrentHashMap<>();


    public static AksClient getAksClient() {
        if (aksClient == null) {
            synchronized (Decrypt.class) {
                if (aksClient == null) {
                    AksClientFactory factory = new AksHttpClientFactory(
                        getAksConfig("access_key"), "172.25.135.243");
                    aksClient = factory.createClient();
                }
            }
        }
        return aksClient;
    }

    public static void shutdown() {
        if (aksClient != null) {
            Field[] fields = AksClient.class.getDeclaredFields();

            for(Field field : fields) {
                if (field.getName().equalsIgnoreCase("reportScheduler") || field.getName().equalsIgnoreCase("heartbeatScheduler")) {
                    boolean accessible = field.isAccessible();

                    try {
                        if (!accessible) {
                            field.setAccessible(true);
                        }

                        ScheduledExecutorService executor = (ScheduledExecutorService)field.get(aksClient);
                        executor.shutdown();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } finally {
                        if (!accessible) {
                            field.setAccessible(false);
                        }

                    }
                }
            }
        }
    }

    public static String index(String value, boolean interruptOnError) throws Exception {
        AksResponse result = getAksClient().index(value.getBytes(StandardCharsets.UTF_8));
        if (result.isSuccess()) {
            return result.getIndexBase64();
        } else {
            logger.warn("get resp fail, code = {}", result.getResponseCode());
            if (interruptOnError) {
                throw new Exception("aks index error , value = [" + value + "] , msg = [" + result.getResponseMessage() + "]");
            } else {
                return "&*(_akserror:" + value;
            }
        }
    }

    public static String getAksConfig(String key) {
        if(aksConf.containsKey(key)){
            return aksConf.get(key);
        } else {
            try (InputStream inputStream = AksUtils.class.getClassLoader()
                .getResourceAsStream("key.properties")) {
                if (inputStream != null) {
                    Properties properties = new Properties();
                    properties.load(inputStream);
                    String property = properties.getProperty(key);
                    aksConf.put(key, property);
                    return property;
                } else {
                    logger.warn("Unable to find config.properties file in the classpath.");
                }
            } catch (IOException e) {
                logger.error("Read ask config error", e);
            }
            return null;
        }
    }
}
