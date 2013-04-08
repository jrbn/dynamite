package nl.vu.cs.querypie.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.WriteToFiles.StandardFileWriter;

public class AppendFileWriter extends StandardFileWriter {

	public AppendFileWriter(ActionContext context, File file) throws IOException {
		writer = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(file, true)));
		// writer = new BufferedOutputStream(new FileOutputStream(file, true));
	}
}
