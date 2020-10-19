package no.ssb.rawdata.converter.util;

import lombok.experimental.UtilityClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@UtilityClass
public class CloneUtil {

    /**
     * Make a "deep clone" of any object by serializing and reading it back as a new object. The object to clone must
     * implement {@link Serializable} .
     *
     * Ref https://alvinalexander.com/java/java-deep-clone-example-source-code/
     */
    public static <T extends Serializable> T deepClone(T object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error cloning object of type " + object.getClass(), e);
        }
    }

}
