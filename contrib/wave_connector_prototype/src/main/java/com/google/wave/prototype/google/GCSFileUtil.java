package com.google.wave.prototype.google;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsUtilFactory;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

public class GCSFileUtil {
    private GcsUtil gcsUtil;

    public GCSFileUtil(PipelineOptions options) {
        gcsUtil = new GcsUtilFactory().create(options);
    }

    public byte[] read(String filePath) throws IOException {
        GcsPath gcsPath = GcsPath.fromUri(filePath);
        SeekableByteChannel seekableByteChannel = gcsUtil.open(gcsPath);
        ByteBuffer fileContent = ByteBuffer.allocate(Long.valueOf(gcsUtil.fileSize(gcsPath)).intValue());
        seekableByteChannel.read(fileContent);

        return fileContent.array();
    }

}
