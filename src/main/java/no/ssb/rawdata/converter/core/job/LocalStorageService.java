package no.ssb.rawdata.converter.core.job;

import io.micronaut.context.annotation.Context;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Async;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.EncryptionMethod;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

@Singleton
@Context
@Slf4j
public class LocalStorageService {

    @EventListener
    @Async
    void onLocalStorageRequest(LocalStorageEvent req) {
        processStorageEvent(req);
    }

    private void processStorageEvent(LocalStorageEvent req) {
        boolean storeAsArchive = req.getLocalStoragePassword() != null;
        Path targetPath = storeAsArchive
          ? req.getPathPrefix()
          : req.getPathPrefix().resolve(currentTimeFragment()).resolve(req.getFileGroupName());

        try {
            Files.createDirectories(targetPath);
        }
        catch (IOException e) {
            throw new LocalStorageServiceException("Error creating target path " + targetPath, e);
        }

        if (storeAsArchive) {
            addFilesToArchive(targetPath.resolve(currentTimeFragment() + ".zip"), req);
        }
        else {
            storeFilesToDisk(targetPath, req);
        }
    }

    private String currentTimeFragment() {
        LocalDateTime now = LocalDateTime.now();
        return String.format("%02d%02d%02d", now.getMonthValue(), now.getDayOfMonth(), now.getHour());
    }

    private void storeFilesToDisk(Path targetPath, LocalStorageEvent req) {
        for (String filename : req.getFiles().keySet()) {
            try {
                Files.write(targetPath.resolve(filename), req.getFiles().get(filename));
            }
            catch (Exception e) {
                throw new LocalStorageServiceException("Error writing file " + filename, e);
            }
        }
    }

    private void addFilesToArchive(Path archivePath, LocalStorageEvent req) {
            try {
                ZipFile zipFile = new ZipFile(archivePath.toFile(), req.getLocalStoragePassword().toCharArray());

                for (String filename : req.getFiles().keySet()) {
                    ZipParameters params = new ZipParameters();
                    params.setEncryptFiles(true);
                    params.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
                    params.setFileNameInZip(req.getFileGroupName() + "/" + filename);
                    params.setCompressionLevel(CompressionLevel.MAXIMUM);

                    zipFile.addStream(new ByteArrayInputStream(req.getFiles().get(filename)), params);
                }
            }
            catch (Exception e) {
                throw new LocalStorageServiceException("Error adding files to archive " + archivePath, e);
            }
    }

    public class LocalStorageServiceException extends RawdataConverterException {
        public LocalStorageServiceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}