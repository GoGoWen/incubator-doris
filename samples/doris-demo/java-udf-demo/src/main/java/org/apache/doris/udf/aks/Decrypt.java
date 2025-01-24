package org.apache.doris.udf.aks;

import com.jd.jr.aks.api.model.AksResponse;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.nio.charset.StandardCharsets;

import static org.apache.doris.udf.util.AksUtils.getAksClient;

/**
 * aks_decrypt
 */
public class Decrypt extends UDF {

    /**
     * aks_decrypt(String)
     *
     * @param value
     * @return
     * @throws Exception
     */
    public String evaluate(String value) throws Exception {
        return evaluate(value, true);
    }

    /**
     * aks_decrypt(String, Boolean)
     *
     * @param value
     * @param interruptOnError
     * @return
     * @throws Exception
     */
    public String evaluate(String value, Boolean interruptOnError) throws Exception {
        try {
            AksResponse result = getAksClient().decrypt(value);
            if (result.isSuccess()) {
                return new String(result.getPlainByte(), StandardCharsets.UTF_8);
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
