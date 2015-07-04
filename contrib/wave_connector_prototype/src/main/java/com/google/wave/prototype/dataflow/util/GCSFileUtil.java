package com.google.wave.prototype.dataflow.util;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

/**
 * A Google Cloud Storage utility which can be used to read the files present in GCS
 * This utility can be used only for the Jobs running in Google Dataflow
 * This makes use of {@code GcsUtil} and {@code GcsPath} to read the file present in GCS
 */
public class GCSFileUtil {
    private GcsUtil gcsUtil;

    public GCSFileUtil(PipelineOptions options) {
        // PipelineOption is required to create GcsUtil
        // hence this can be used only for Google Dataflow jobs
        gcsUtil = new GcsUtilFactory().create(options);
    }

    public byte[] read(String filePath) throws Exception {
        GcsPath gcsPath = GcsPath.fromUri(filePath);
        SeekableByteChannel seekableByteChannel = gcsUtil.open(gcsPath);
        // Allocating ByteBuffer based on the file size
        ByteBuffer fileContent = ByteBuffer.allocate(Long.valueOf(gcsUtil.fileSize(gcsPath)).intValue());
        seekableByteChannel.read(fileContent);

        return fileContent.array();
    }

}
