package org.apache.doris.udf.aks;

import com.jd.jr.aks.api.model.AksResponse;
import org.apache.hadoop.hive.ql.exec.UDF;

import static org.apache.doris.udf.util.AksUtils.getAksClient;
import static org.apache.doris.udf.util.AksUtils.getAksConfig;

/**
 * aks_decrypt
 */
public class Encrypt extends UDF {

    /**
     * aks_encrypt(String)
     *
     * @param value
     * @return
     * @throws Exception
     */
    public String evaluate(String value) throws Exception {
        return evaluate(value, true);
    }

    /**
     * aks_encrypt(String, Boolean)
     *
     * @param value
     * @param interruptOnError
     * @return
     * @throws Exception
     */
    public String evaluate(String value, Boolean interruptOnError) throws Exception {
        try {
            AksResponse result = getAksClient().encrypt(getAksConfig("key_id"), value.getBytes());
            if (result.isSuccess()) {
                return result.getCipherText();
            } else {
                throw new Exception("aks decrypt error , value = [" + value + "] , msg = [" + result.getResponseMessage() + "]");
            }
        } catch (Exception e) {
            if (interruptOnError) {
                throw e;
            } else {
                return "&*(_akserror:" + value;
            }
        }
    }
}
