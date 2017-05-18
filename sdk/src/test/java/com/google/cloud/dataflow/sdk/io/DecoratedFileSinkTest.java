package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.dataflow.sdk.io.DecoratedFileSink.DecoratedFileWriter;
import com.google.cloud.dataflow.sdk.io.DecoratedFileSink.WriterOutputDecorator;
import com.google.cloud.dataflow.sdk.io.DecoratedFileSink.WriterOutputDecoratorFactory;
import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileResult;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.util.MimeTypes;

@RunWith(JUnit4.class)
public class DecoratedFileSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String TEMPORARY_FILENAME_SEPARATOR = "-temp-";
  private String baseOutputFilename = "output";
  private String baseTemporaryFilename = "temp";

  private String appendToTempFolder(String filename) {
    return Paths.get(tmpFolder.getRoot().getPath(), filename).toString();
  }

  private String getBaseOutputFilename() {
    return appendToTempFolder(baseOutputFilename);
  }

  private String getBaseTempFilename() {
    return appendToTempFolder(baseTemporaryFilename);
  }

  /**
   * Assert that a file contains the lines provided, in the same order as expected.
   */
  private void assertFileContains(List<String> expected, String filename) throws Exception {
    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
      List<String> actual = new ArrayList<>();
      for (;;) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        actual.add(line);
      }
      assertEquals(expected, actual);
    }
  }

  /**
   * {@link DecoratedFileWriter} writes to the {@link WriterOutputDecorator} provided by
   * {@link WriterOutputDecoratorFactory}.
   */
  @Test
  public void testDecoratedFileWriter() throws Exception {
    final String testUid = "testId";
    final String expectedFilename =
        getBaseOutputFilename() + TEMPORARY_FILENAME_SEPARATOR + testUid;
    final DecoratedFileWriter<String> writer =
        new DecoratedFileSink<String>(getBaseOutputFilename(), "txt", TextIO.DEFAULT_TEXT_CODER,
            "h", "f", new SimpleDecoratorFactory()).createWriteOperation(null).createWriter(null);

    final List<String> expected = new ArrayList<>();
    expected.add("hh");
    expected.add("");
    expected.add("aa");
    expected.add("");
    expected.add("bb");
    expected.add("");
    expected.add("ff");
    expected.add("");

    writer.open(testUid);
    writer.write("a");
    writer.write("b");
    final FileResult result = writer.close();

    assertEquals(expectedFilename, result.getFilename());
    assertFileContains(expected, expectedFilename);
  }

  private static class SimpleDecoratorFactory implements WriterOutputDecoratorFactory {
    @Override
    public WriterOutputDecorator create(OutputStream out) throws IOException {
      return new SimpleDecorator(out);
    }

    @Override
    public String getMimeType() {
      return MimeTypes.TEXT;
    }

    private static class SimpleDecorator extends WriterOutputDecorator {
      public SimpleDecorator(final OutputStream out) {
        // OutputStream just writes each byte twice.
        super(new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            out.write(b);
            out.write(b);
          }
        });
      }
    }
  }
}
