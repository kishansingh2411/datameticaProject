package com.cablevision.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UntarUtil {

	private final static Logger logger = LoggerFactory.getLogger(UntarUtil.class);

	private static final String DM_COMPRESSED_INPUT_CODEC = "dm.compressed.input.codec";

	public static void extractArchive(Configuration conf, Path archiveFilePath, Path outputPath) throws Exception {

		try {
				FileSystem fileSystem = archiveFilePath.getFileSystem(conf);
				
				if (fileSystem.isDirectory(archiveFilePath))
				{
					FileStatus[] fileStatuses = fileSystem.listStatus(archiveFilePath);
					for (FileStatus fStatus: Arrays.asList(fileStatuses))
					{
						Path filePath = fStatus.getPath();
						
						if (filePath.getName().endsWith(".tar.gz") )
						{
							Path newOutputPath = new Path(outputPath, filePath.getName().substring(0, filePath.getName().indexOf(".tar.gz")));
							extractFile(conf, filePath,  newOutputPath);
						}
						else if  (filePath.getName().endsWith(".tgz"))
						{
							Path newOutputPath = new Path(outputPath, filePath.getName().substring(0, filePath.getName().indexOf(".tgz")));
							extractFile(conf, filePath,  newOutputPath);
						}
						
					}
				}
				else
				{
					if (archiveFilePath.getName().endsWith(".tar.gz") )
					{
						extractFile(conf, archiveFilePath,  new Path(outputPath, archiveFilePath.getName().substring(0, archiveFilePath.getName().indexOf(".tar.gz"))));
					}
					else if  (archiveFilePath.getName().endsWith(".tgz"))
					{
						extractFile(conf, archiveFilePath,  new Path(outputPath, archiveFilePath.getName().substring(0, archiveFilePath.getName().indexOf(".tgz"))));
					}
				}
				
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw e;
		}
		
	}


	private static void extractFile(Configuration conf, Path archivePath, Path outputPath)
			throws IOException, ArchiveException, Exception {
		
		OutputStream fout = null;
		ArchiveInputStream archiveInputStream = null;
		
		try {
			String fileName = archivePath.getName();
			logger.info("archive file name "+fileName);
			logger.info("output path "+outputPath);
			FileSystem outputFileSystem = outputPath.getFileSystem(conf);
			outputFileSystem.mkdirs(outputPath);
			archiveInputStream = getArchiveInputStream(conf,
					archivePath);
			ArchiveEntry archiveEntry = null;
			int bufferSize = 67108864;

			while ((archiveEntry = archiveInputStream.getNextEntry()) != null) {
				String entryName = archiveEntry.getName();
				Path entryOutputPath = new Path(outputPath, entryName);
				if (archiveEntry.isDirectory()) {
					outputFileSystem.mkdirs(entryOutputPath);
				} else {
					 fout = outputFileSystem
							.create(entryOutputPath);

					byte buf[] = new byte[bufferSize];
					logger.info("started extracting file "+entryName);
					int bytesRead = archiveInputStream.read(buf);
					while (bytesRead >= 0) {
						fout.write(buf, 0, bytesRead);
						bytesRead = archiveInputStream.read(buf);
						logger.info(".......");
					}
					logger.info("done extracting file "+entryName);
					fout.close();
				}
			}
			
			archiveInputStream.close();
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw e;
		}
		finally
		{
			if (fout != null) 
			{
				fout.close();
			}
			
			if (archiveInputStream != null)
			{
				archiveInputStream.close();
			}
		}
	}


	private static ArchiveInputStream getArchiveInputStream(Configuration conf, Path archivePath) throws IOException, ArchiveException {
		String fileName = archivePath.getName();

		TarArchiveInputStream tarIn = null;

		if (fileName.endsWith("tgz") || fileName.endsWith("tar.gz")) {
			logger.info("archive file :"+fileName);
			FileSystem inputFileSystem = archivePath.getFileSystem(conf);
			Class<? extends CompressionCodec> codecClass = getCompressorCodecClass(
					conf, DM_COMPRESSED_INPUT_CODEC, GzipCodec.class);
			CompressionCodec codec = (CompressionCodec) ReflectionUtils
					.newInstance(codecClass, conf);
			InputStream in = inputFileSystem.open(archivePath);
			in = codec.createInputStream(in);
			BufferedInputStream bufIn = new BufferedInputStream(in);
			tarIn = new TarArchiveInputStream(bufIn);
		}
		else
		{
			System.out.println("Input archive file should be in either tar.gz or tgz format");
			System.exit(1);
		}
		
		return tarIn;
	}

	public static Class<? extends CompressionCodec> getCompressorCodecClass(
			Configuration conf, String confKey, Class<? extends CompressionCodec> defaultValue) {
		Class<? extends CompressionCodec> codecClass = defaultValue;
		String name = conf
				.get(confKey);
		if (name != null) {
			try {
				codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
			} catch (ClassNotFoundException e) {
				logger.error(e.getMessage(), e);
				throw new IllegalArgumentException("Compression codec " + name
						+ " was not found.", e);
			}
		}
		return codecClass;
	}

	
	public static void main(String[] args) throws Exception {
		
		String archiveFilePath = args[0];
		String outputPath = args[1];
		logger.info("Archive file to be extracted "+archiveFilePath);
		logger.info("Output path "+outputPath);
		
		Configuration conf = new Configuration();

		try {
		
			Path path = new Path(archiveFilePath);
			extractArchive(conf, path, new Path(outputPath));
		
		} catch (IOException e) {
			
			logger.error(e.getMessage(), e);
			throw e;
		}
		
	}

}
