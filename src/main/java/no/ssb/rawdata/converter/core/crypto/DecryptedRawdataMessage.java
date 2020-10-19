package no.ssb.rawdata.converter.core.crypto;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.payload.encryption.EncryptionClient;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class DecryptedRawdataMessage implements RawdataMessage {

    private final RawdataMessage delegate;
    private final EncryptionClient encryptionClient;
    private final Map<String, byte[]> decryptedData = new LinkedHashMap<>();

    public DecryptedRawdataMessage(RawdataMessage rawdataMessage, EncryptionClient encryptionClient, byte[] secretKey) {
        this.delegate = rawdataMessage;
        this.encryptionClient = encryptionClient;
        try {
            decryptData(secretKey);
        } catch (Exception e) {
            throw new DecryptRawdataMessageException(
                    String.format("Failed to decrypt message with ulid=%s. If this is due to the message not being encrypted, disable decryption to fix", rawdataMessage.ulid().toString()), e
            );
        }
    }

    private void decryptData(byte[] secretKey) {
        for (Map.Entry<String, byte[]> entry : delegate.data().entrySet()) {
            byte[] decryptedContent = encryptionClient.decrypt(secretKey, entry.getValue());
            decryptedData.put(entry.getKey(), decryptedContent);
        }
    }

    @Override
    public ULID.Value ulid() {
        return delegate.ulid();
    }

    @Override
    public long timestamp() {
        return delegate.timestamp();
    }

    @Override
    public String orderingGroup() {
        return delegate.orderingGroup();
    }

    @Override
    public long sequenceNumber() {
        return delegate.sequenceNumber();
    }

    @Override
    public String position() {
        return delegate.position();
    }

    @Override
    public Set<String> keys() {
        return delegate.keys();
    }

    @Override
    public byte[] get(String key) {
        return decryptedData.get(key);
    }

    @Override
    public Map<String, byte[]> data() {
        return decryptedData;
    }

    public static class DecryptRawdataMessageException extends RuntimeException {
        public DecryptRawdataMessageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
