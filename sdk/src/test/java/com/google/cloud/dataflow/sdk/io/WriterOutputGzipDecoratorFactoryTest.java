package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.dataflow.sdk.io.DecoratedFileSink.WriterOutputDecorator;

@RunWith(JUnit4.class)
public class WriterOutputGzipDecoratorFactoryTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testCreateAndWrite() throws FileNotFoundException, IOException {
    final WriterOutputGzipDecoratorFactory factory = WriterOutputGzipDecoratorFactory.getInstance();
    final File file = tmpFolder.newFile("test.gz");
    final OutputStream fos = new FileOutputStream(file);
    final WriterOutputDecorator decorator = factory.create(fos);
    decorator.write("abc\n".getBytes(StandardCharsets.UTF_8));
    decorator.out.write("123\n".getBytes(StandardCharsets.UTF_8));
    decorator.finish();
    decorator.close();
    // Read Gzipped data back in using standard API.
    final BufferedReader br =
        new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)),
            StandardCharsets.UTF_8.name()));
    assertEquals("First line should read 'abc'", "abc", br.readLine());
    assertEquals("Second line should read '123'", "123", br.readLine());
  }

}
