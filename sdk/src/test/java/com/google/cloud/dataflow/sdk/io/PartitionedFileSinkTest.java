package com.google.cloud.dataflow.sdk.io;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.KV;

import com.zendesk.dataflow.io.PartitionedFileSink;
import com.zendesk.dataflow.io.PartitionedFileSink.FileResult;
import com.zendesk.dataflow.io.PartitionedFileSink.PartitionedFileWriteOperation;
import com.zendesk.dataflow.io.PartitionedFileSink.PartitionedFileWriteOperation.TemporaryFileRetention;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * Tests for PartitionedFileSink.
 */
public class PartitionedFileSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSinkTest.class);

  private String baseFilename = "basename";
  private String testExtension = ".txt";

  private String appendToTempFolder(String filename) {
    return Paths.get(tmpFolder.getRoot().getPath(), filename).toString();
  }

  private String getFullBaseFilename() {
    return appendToTempFolder(baseFilename);
  }

  private String testUid = "testId";

  protected final List<String> filesToStrings(List<File> files) {
    List<String> filenames = new ArrayList<>();
    for(File file : files) {
      filenames.add(file.toString());
    }
    return filenames;
  }
  /**
   * PartitionedFileWriter opens the correct files, writes the header, footer, and elements in the
   * correct order, and returns the correct filenames.
   */
  @Test
  public void testWriter() throws Exception {
    LOG.info("Testing the Write operations");
    PartitionedFileSink.PartitionedFileWriter writer = buildWriter();

    List<KV<String, Iterable<String>>> values = Arrays.asList(
      KV.of("dir1/file1", Arrays.asList("One", "Two", "Three")),
      KV.of("dir2/file2", Arrays.asList("One2", "Two2", "Three2"))
    );

    writer.open(testUid);
    for (KV<String, Iterable<String>> value : values) {
      writer.write(value);
    }
    FileResult result = writer.close();


    List<String> expectedFilenames = new ArrayList<>();
    for (KV<String, Iterable<String>> value : values) {
      String tmpDirLocation = PartitionedFileWriteOperation.TEMPORARY_FILENAME_SEPARATOR + testUid + "/";
      String expectedFilename = getFullBaseFilename().replaceAll("/[^/]+$", tmpDirLocation + baseFilename + value.getKey());
      expectedFilenames.add(expectedFilename);
      List<String> expectedContent = new ArrayList<>();
      value.getValue().forEach(expectedContent::add);
      assertFileContains(expectedContent, expectedFilename);
    }
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
   * Write lines to a file.
   */
  private void writeFile(List<String> lines, File file) throws Exception {
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(file))) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  /**
   * Removes temporary files when temporary and output filenames differ.
   */
  @Test
  public void testRemoveWithTempFilename() throws Exception {
    testRemoveTemporaryFiles(3, getFullBaseFilename());
  }

  // /**
  //  * Finalize copies temporary files to output files and removes any temporary files.
  //  */
  @Test
  public void testFinalizeWithNoRetention() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    boolean retainTemporaryFiles = false;
    runFinalize(buildWriteOperationForFinalize(retainTemporaryFiles), files, retainTemporaryFiles);
  }

  /**
   * Finalize retains temporary files when requested.
   */
  @Test
  public void testFinalizeWithRetention() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    boolean retainTemporaryFiles = true;
    runFinalize(buildWriteOperationForFinalize(retainTemporaryFiles), files, retainTemporaryFiles);
  }

  /**
   * Finalize can be called repeatedly.
   */
  @Test
  public void testFinalizeMultipleCalls() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = buildWriteOperationForFinalize(false);
    runFinalize(writeOp, files, false);
    runFinalize(writeOp, files, false);
  }

  /**
   * Finalize can be called when some temporary files do not exist and output files exist.
   */
  @Test
  public void testFinalizeWithIntermediateState() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = buildWriteOperationForFinalize(false);
    runFinalize(writeOp, files, false);

    // create a temporary file
    createFileRecursive(writeOp.buildTemporaryFilename("part1", getFullBaseFilename(), testUid));
    runFinalize(writeOp, files, false);
  }

  /**
   * Build a PartitionedFileWriteOperation with default values and the specified retention policy.
   */
  private PartitionedFileSink.PartitionedFileWriteOperation buildWriteOperationForFinalize(
      boolean retainTemporaryFiles) throws Exception {
    TemporaryFileRetention retentionPolicy =
        retainTemporaryFiles ? TemporaryFileRetention.KEEP : TemporaryFileRetention.REMOVE;
    return buildWriteOperation(retentionPolicy);
  }

  /**
   * Generate n temporary files using the temporary file pattern of PartitionedFileWriter.
   */
  private List<File> generateTemporaryFilesForFinalize(int numFiles) throws Exception {
    List<File> temporaryFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String temporaryFilename =
          PartitionedFileWriteOperation.buildTemporaryFilename(Integer.toString(i), getFullBaseFilename(), testUid);
      File tmpFile = createFileRecursive(temporaryFilename);
      temporaryFiles.add(tmpFile);
    }

    return temporaryFiles;
  }

  /**
   * Finalize and verify that files are copied and temporary files are optionally removed.
   */
  private void runFinalize(PartitionedFileSink.PartitionedFileWriteOperation writeOp, List<File> temporaryFiles,
      boolean retainTemporaryFiles) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();

    int numFiles = temporaryFiles.size();

    List<File> outputFiles = new ArrayList<>();
    List<FileResult> fileResults = new ArrayList<>();
    List<String> outputFilenames = writeOp.generateDestinationFilenames(filesToStrings(temporaryFiles));

    // Create temporary output bundles and output File objects
    for (int i = 0; i < numFiles; i++) {
      fileResults.add(new FileResult(Arrays.asList(temporaryFiles.get(i).toString())));
      outputFiles.add(new File(outputFilenames.get(i)));
    }

    writeOp.finalize(fileResults, options);

    for (int i = 0; i < numFiles; i++) {
      assertTrue(outputFiles.get(i).exists());
      assertEquals(retainTemporaryFiles, temporaryFiles.get(i).exists());
    }
  }

  /**
   * Create n temporary and output files and verify that removeTemporaryFiles only
   * removes temporary files.
   */
  private void testRemoveTemporaryFiles(int numFiles, String baseFilePath)
      throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = buildWriteOperation(baseFilePath);

    List<File> temporaryFiles = new ArrayList<>();
    List<File> outputFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String filename = writeOp.buildTemporaryFilename(Integer.toString(i), baseFilePath, testUid);
      File tmpFile = createFileRecursive(filename);
      temporaryFiles.add(tmpFile);
      File outputFile = tmpFolder.newFile(baseFilename + i);
      outputFiles.add(outputFile);
    }

    writeOp.removeTemporaryFiles(options);

    for (int i = 0; i < numFiles; i++) {
      assertFalse(temporaryFiles.get(i).exists());
      assertTrue(outputFiles.get(i).exists());
    }
  }

  /**
   * Output files are copied to the destination location with the correct names and contents.
   */
  @Test
  public void testMoveToOutputFiles() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = buildWriteOperation();

    List<String> inputContents = Arrays.asList("3", "2", "1");
    List<String> inputFilePaths = new ArrayList<>();
    List<String> expectedOutputPaths = new ArrayList<>();

    for (int i = 0; i < inputContents.size(); i++) {
      // Generate output paths.
      // File outputFile = tmpFolder.newFile(expectedOutputFilenames.get(i));
      String outputFilename = getFullBaseFilename() + "part" + Integer.toString(i) + testExtension;
      expectedOutputPaths.add(outputFilename);

      // Generate and write to input paths.
      String inputPath = writeOp.buildTemporaryFilename("part" + Integer.toString(i), getFullBaseFilename(), testUid);
      File inputTmpFile = createFileRecursive(inputPath);
      List<String> lines = Arrays.asList(inputContents.get(i));
      writeFile(lines, inputTmpFile);
      inputFilePaths.add(inputPath);
    }

    // Copy input files to output files.
    List<String> actual = writeOp.moveToOutputFiles(inputFilePaths, options);

    // Assert that the expected paths are returned.
    assertThat(expectedOutputPaths, containsInAnyOrder(actual.toArray()));

    // Assert that the contents were copied.
    for (int i = 0; i < expectedOutputPaths.size(); i++) {
      assertFileContains(Arrays.asList(inputContents.get(i)), expectedOutputPaths.get(i));
    }
  }

  private File createFileRecursive(String filename) throws Exception {
    File file = new File(filename);
    file.getParentFile().mkdirs();
    file.createNewFile();
    return file;
  }


  /**
   * Build a PartitionedFileSink with default options.
   */
  private PartitionedFileSink buildSink() {
    return new PartitionedFileSink(getFullBaseFilename(), testExtension);
  }

  // /**
  //  * Build a PartitionedFileSink with default options and the given shard template.
  //  */
  private PartitionedFileSink buildSink(String shardTemplate) {
    return new PartitionedFileSink(getFullBaseFilename(), testExtension, shardTemplate);
  }

  /**
   * Build a PartitionedFileWriteOperation with default options and the given file retention policy.
   */
  private PartitionedFileSink.PartitionedFileWriteOperation buildWriteOperation(
      TemporaryFileRetention fileRetention) {
    PartitionedFileSink sink = buildSink();
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = new PartitionedFileSink.PartitionedFileWriteOperation(sink, getFullBaseFilename(), fileRetention);
    writeOp.setId(testUid);
    return writeOp;
  }

  /**
   * Build a PartitionedFileWriteOperation with default options and the given base temporary filename.
   */
  private PartitionedFileSink.PartitionedFileWriteOperation buildWriteOperation(String baseFilename) {
    PartitionedFileSink sink = buildSink();
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = new PartitionedFileSink.PartitionedFileWriteOperation(sink, baseFilename);
    writeOp.setId(testUid);
    return writeOp;
  }

  /**
   * Build a write operation with the default options for it and its parent sink.
   */
  private PartitionedFileSink.PartitionedFileWriteOperation buildWriteOperation() {
    PartitionedFileSink sink = buildSink();
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = new PartitionedFileSink.PartitionedFileWriteOperation(sink, getFullBaseFilename(), TemporaryFileRetention.REMOVE);
    writeOp.setId(testUid);
    return writeOp;
  }

  /**
   * Build a writer with the default options for its parent write operation and sink.
   */
  private PartitionedFileSink.PartitionedFileWriter buildWriter() {
    PartitionedFileSink.PartitionedFileWriteOperation writeOp = buildWriteOperation(TemporaryFileRetention.REMOVE);
    return new PartitionedFileSink.PartitionedFileWriter(writeOp);
  }
}

