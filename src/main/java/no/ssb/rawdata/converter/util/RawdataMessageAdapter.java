package no.ssb.rawdata.converter.util;

import com.fasterxml.jackson.core.type.TypeReference;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wraps a RawdataMessage and provides utility methods for convenience access and debugging functionality.
 */
public class RawdataMessageAdapter {
    private final static String MANIFEST_ITEM_NAME = "manifest.json";

    private final RawdataMessage message;

    public RawdataMessageAdapter(RawdataMessage message) {
        this.message = message;
    }

    public RawdataMessage getMessage() {
        return message;
    }

    public ULID.Value getUlid() {
        return message.ulid();
    }

    public String getPosition() {
        return message.position();
    }

    public String toIdString() {
        return posAndIdOf(message);
    }

    public boolean hasItem(String itemName) {
        return message.keys().contains(itemName);
    }

    public Optional<String> getTopic() {
        return getAllItemMetadata().values().stream()
          .filter(m -> m.getTopic().isPresent())
          .findFirst()
          .flatMap(m -> m.getTopic());
    }

    public Optional<byte[]> findItem(String itemName) {
        return Optional.ofNullable(message.get(itemName));
    }

    public Optional<String> findItemAsString(String itemName) {
        try {
            return Optional.of(getItemAsString(itemName));
        }
        catch (NoRawdataMessageItemFoundException e) {
            return Optional.empty();
        }
    }

   public byte[] getItem(String itemName) {
        return findItem(itemName)
          .orElseThrow(() -> new NoRawdataMessageItemFoundException(itemName, message));
    }

    public String getItemAsString(String itemName) {
        return new String(getItem(itemName));
    }

    public Optional<byte[]> findManifest() {
        return findItem(MANIFEST_ITEM_NAME);
    }

    public boolean hasManifest() {
        return hasItem(MANIFEST_ITEM_NAME);
    }

    public byte[] getManifest() {
        return getItem(MANIFEST_ITEM_NAME);
    }

    public String getManifestAsString() {
        return getItemAsString(MANIFEST_ITEM_NAME);
    }

    /**
     * Prints all parts to stdout.
     *
     * Convenience method for debug purposes only.
     */
    public void printAllParts() {
        System.out.println(allPrintablePartsToString());
    }

    public static String toDebugString(RawdataMessage message) {
        return new RawdataMessageAdapter(message).allPrintablePartsToString();
    }

    /**
     * Dump all parts to a String.
     *
     * Convenience method for debug purposes only.
     */
    public String allPrintablePartsToString() {
        Map<String, ItemMetadata> allMetadata = getAllItemMetadata();
        StringBuilder sb = new StringBuilder();
        for (String itemName : message.keys()) {
            ItemMetadata itemMetadata = allMetadata.get(itemName);
            sb.append(itemName + System.lineSeparator() + "-".repeat(79) + System.lineSeparator());
            if (itemMetadata != null && !itemMetadata.isPrintable()) {
                sb.append("*** NOT PRINTABLE CONTENT (" + itemMetadata.getContentType()  + ") ***");
            }
            else {
                sb.append(getItemAsString(itemName));
            }
            sb.append(System.lineSeparator().repeat(2));
        }

        return sb.toString();
    }

    public static String posAndIdOf(RawdataMessage msg) {
        return (msg == null)
          ? "n/a"
          : String.format("pos=%s, ulid=%s", msg.position(), msg.ulid());
    }

    /**
     * Convenience method to print a RawdataMessage to stdout
     */
    public static void print(RawdataMessage message) {
        new RawdataMessageAdapter(message).printAllParts();
    }

    /**
     * Convenience method to write the contents of a RawdataMessage to working directory
     */
    public static void write(RawdataMessage message) {
        new RawdataMessageAdapter(message).writeAllPartsToWorkdir();
    }

    /**
     * Convenience method to write the contents of a RawdataMessage to given path
     */
    public static void write(RawdataMessage message, Path targetRootPath) throws IOException {
        new RawdataMessageAdapter(message).writeAllParts(targetRootPath);
    }

    /**
     * Write all items of a rawdata message as files to the specified Path
     */
    public void writeAllParts(Path targetRootPath) throws IOException {
        Path targetPath = targetRootPath.resolve(message.ulid().toString());
        Files.createDirectories(targetPath);
        for (String entryId : message.keys()) {
            Files.write(targetPath.resolve(entryId), message.get(entryId));
        }
    }

    /**
     * Write all items of a rawdata message as files to the current working directory
     *
     * Convenience method for debug purposes only. Swallows exceptions.
     */
    public void writeAllPartsToWorkdir() {
        Path targetPath = Path.of("rawdata");
        try {
            writeAllParts(targetPath);
        }
        catch (Exception e) {
            System.out.println("Error writing rawdata message to workdir " + targetPath);;
        }
    }

    /**
     * Searches for item metadata in the manifest file.
     *
     * @param itemName name of the item to find metadata for
     * @return Optional ItemMetadata if found, or empty if no manifest was found, or if no item metadata for the
     *  specified itemName could be found in the manifest
     */
    public Optional<ItemMetadata> findItemMetadata(String itemName) {
        return Optional.ofNullable(getAllItemMetadata().get(itemName));
    }

    /**
     * Return metadata for all items listed in the manifest
     *
     * @return all metadata items or empty Map if no manifest found. Never null.
     */
    public Map<String, ItemMetadata> getAllItemMetadata() {
        String manifestJson = findItemAsString(MANIFEST_ITEM_NAME).orElse(null);
        if (manifestJson == null) {
            return Collections.EMPTY_MAP;
        }

        List<Map<String,Object>> manifestItems = Json.toObject(new TypeReference<List<Map<String,Object>>>() {}, manifestJson);

        return manifestItems.stream()
          .map(ItemMetadata::new)
          .collect(Collectors.toMap(
            ItemMetadata::getContentKey,
            Function.identity())
          );
    }

    public static class NoRawdataMessageItemFoundException extends RuntimeException {
        public NoRawdataMessageItemFoundException(String itemName, RawdataMessage msg) {
            super("No item " + itemName + " found in RawdataMessage " + msg.ulid());
        }
    }

    /**
     * Metadata associated with a RawdataMessage item
     */
    public static class ItemMetadata {
        private final static String NO_VALUE = "null";
        private final static String TOPIC_KEY = "topic";
        private final static String CONTENT_KEY = "content-key";
        private final static String CONTENT_TYPE_KEY = "content-type";
        private final static Set<String> PRINTABLE_CONTENT_TYPES = Set.of(
          "json", "csv", "xml", "text", NO_VALUE
        );
        private final Map<String, Object> metadataMap;
        private final Map<String, Object> schemaMap;

        public ItemMetadata(Map<String, Object> manifestItemMap) {
            this.metadataMap = (Map<String,Object>) manifestItemMap.get("metadata");
            this.schemaMap = (Map<String,Object>) manifestItemMap.get("schema");
        }

        private String metadataStringValueOf(String key) {
            return (String) metadataMap.getOrDefault(key,  NO_VALUE);
        }

        public Optional<String> getTopic() {
            return Optional.ofNullable((String) metadataMap.get(TOPIC_KEY));
        }

        public String getContentKey() {
            return metadataStringValueOf(CONTENT_KEY);
        }

        public String getContentType() {
            return metadataStringValueOf(CONTENT_TYPE_KEY).toLowerCase();
        }

        public Map<String, Object> getMetadataMap() {
            return metadataMap;
        }

        public Map<String, Object> getSchemaMap() {
            return schemaMap;
        }

        public boolean isPrintable() {
            String contentType = getContentType();
            return PRINTABLE_CONTENT_TYPES.stream()
              .anyMatch(t -> contentType.contains(t));
        }
    }

}
