// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.util;

import org.apache.doris.thrift.TBDPUserInfo;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public class RSAUtil {
    private static final Logger LOG = LogManager.getLogger(RSAUtil.class);
    public static final String KEY_ALGORIHTM = "RSA";
    private static final int MAX_ENCRYPT_BLOCK = 117;
    private static final int MAX_DECRYPT_BLOCK = 128;
    private static PrivateKey DORIS_RSA_PRIVATE_KEY = null;
    private static ThreadLocal<Map<String, Cipher>> decryptCipherMap = new ThreadLocal<>();
    private static ThreadLocal<Cipher> decryptUserKeyCipher = new ThreadLocal<>();
    private static Map<String, PublicKey> serviceToPublicKeyMap = Maps.newHashMap();
    public static byte[] decryptBase64(String key) {
        return Base64.getDecoder().decode(key);
    }

    public static void init(String propFile) throws IOException {
        Properties props = new Properties();
        FileReader reader = null;
        try {
            reader = new FileReader(propFile);
            props.load(reader);
        } catch (Exception e) {
            LOG.warn("read service.conf file failed", e);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        initServicePublicKeyCertificate(props);
        initServicePrivateKeyCertificate();
    }

    public static void initServicePrivateKeyCertificate() {
        try {
            String privateKeyStr = System.getenv("DORIS_RSA_PRIVATE_KEY");
            if (!Strings.isNullOrEmpty(privateKeyStr)) {
                byte[] privateKeyByte = decryptBase64(privateKeyStr);
                PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyByte);
                KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORIHTM);
                DORIS_RSA_PRIVATE_KEY = keyFactory.generatePrivate(keySpec);
            } else {
                LOG.warn("DORIS_RSA_PRIVATE_KEY is not set in system env");
            }
        } catch (Exception e) {
            LOG.warn("init private key certificate for doris service failed", e);
        }
    }

    public static void initServicePublicKeyCertificate(Properties properties) {
        for (String key : properties.stringPropertyNames()) {
            try {
                String publicKeyStr = properties.getProperty(key);
                byte[] publicKeyByte = decryptBase64(publicKeyStr);
                X509EncodedKeySpec keySpec = new X509EncodedKeySpec(publicKeyByte);
                KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORIHTM);
                serviceToPublicKeyMap.put(key, keyFactory.generatePublic(keySpec));
            } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                LOG.warn("init public key certificate for service {} failed", key, e);
            }
        }
    }

    public static PublicKey getPublicKey(String serviceName)  throws NoSuchAlgorithmException {
        PublicKey publicKey = serviceToPublicKeyMap.get(serviceName);
        if (publicKey == null) {
            throw new NoSuchAlgorithmException("unable to get public key certificate for " + serviceName);
        }
        return publicKey;
    }

    public static TBDPUserInfo decrypt(String serviceName, String text)
            throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException,
            BadPaddingException, IOException, TException {
        Map<String, Cipher> localDecryptCipherMap = decryptCipherMap.get();
        if (localDecryptCipherMap == null) {
            localDecryptCipherMap = Maps.newHashMap();
            decryptCipherMap.set(localDecryptCipherMap);
        }
        Cipher decryptCipher = localDecryptCipherMap.get(serviceName);
        if (decryptCipher == null) {
            decryptCipher = Cipher.getInstance(KEY_ALGORIHTM);
            decryptCipher.init(Cipher.DECRYPT_MODE, getPublicKey(serviceName));
            localDecryptCipherMap.put(serviceName, decryptCipher);
        }
        byte[] textBytes = decryptBase64(text);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        for (int i = 0, offset = 0; textBytes.length - offset > 0; i++, offset = MAX_DECRYPT_BLOCK * i) {
            byte[] tempBytes = textBytes.length - offset > MAX_DECRYPT_BLOCK ?
                    decryptCipher.doFinal(textBytes, offset, MAX_DECRYPT_BLOCK) :
                    decryptCipher.doFinal(textBytes, offset, textBytes.length - offset);
            byteArrayOutputStream.write(tempBytes);
        }
        byteArrayOutputStream.close();
        TBDPUserInfo userInfo = new TBDPUserInfo();
        TDeserializer deserializer = new TDeserializer();
        deserializer.deserialize(userInfo, byteArrayOutputStream.toByteArray());
        return userInfo;
    }

    public static String decryptUserKey(String text)
            throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException,
            BadPaddingException, IOException {
        Cipher localDecryptCipher = decryptUserKeyCipher.get();
        if (localDecryptCipher == null) {
            localDecryptCipher = Cipher.getInstance(KEY_ALGORIHTM);
            localDecryptCipher.init(Cipher.DECRYPT_MODE, DORIS_RSA_PRIVATE_KEY);
            decryptUserKeyCipher.set(localDecryptCipher);
        }
        byte[] textBytes = decryptBase64(text);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        for (int i = 0, offset = 0; textBytes.length - offset > 0; i++, offset = MAX_DECRYPT_BLOCK * i) {
            byte[] tempBytes = textBytes.length - offset > MAX_DECRYPT_BLOCK ?
                    localDecryptCipher.doFinal(textBytes, offset, MAX_DECRYPT_BLOCK) :
                    localDecryptCipher.doFinal(textBytes, offset, textBytes.length - offset);
            byteArrayOutputStream.write(tempBytes);
        }
        byteArrayOutputStream.close();
        return byteArrayOutputStream.toString();
    }
}