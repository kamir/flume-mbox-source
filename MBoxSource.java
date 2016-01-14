//Some standard java classes we'll need:
import java.lang.String;
import java.lang.reflect.Array;

import java.util.logging.Logger;
import java.util.regex.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

//Endeca classes to import:
import com.endeca.edf.adapter.Adapter;
import com.endeca.edf.adapter.AdapterConfig;
import com.endeca.edf.adapter.AdapterHandler;
import com.endeca.edf.adapter.AdapterException;
import com.endeca.edf.adapter.Record;
import com.endeca.edf.adapter.PVal;

/**
 * This Flume-Source reads email messages
 * from an mbox file and converts each message 
 * to a Flume-Event.
 *
 * To compile this class type:
 *
 * javac -classpath $FLUME_LIBS MBoxSource.java
 */
public class MBoxSource implements Adapter
{

	//Constants:

	//Enumeration of the property names we'll use:
	private static String PROP_NAME_SENDER				= "Sender";
	private static String PROP_NAME_DATE					= "Message Date";
	private static String PROP_NAME_SENDER_INFO		= "Sender Info";
	private static String PROP_NAME_BODY					= "Body";

	/**
	 * This is the logger for our adapter. Forge will insert any logs from
	 * this logger into its log files, specifying that they came from
	 * "MBox Adapter".
	 */
	private static Logger log = Logger.getLogger("MBox Adapter");

	/**
	 * This regular expression will be used to extract fields from the From
	 * line of each message. It matches the word "From" followed by a
	 * space, followed by a sequence of non-whitespace characters which
	 * constitute the sender, followed by some whitespace and a string of
	 * exactly 24 characters which consitutes the date and possibly followed
	 * by a string of characters containing other information about the
	 * sender.
	 */
	private static Pattern fromLineRegex
		= Pattern.compile("From (\\S*)\\s*(.{24})(.*)");

	/**********************************
	 * Adapter Interface Implementation
	 **********************************/

	public void execute(AdapterConfig config, AdapterHandler handler)
		throws AdapterException
	{
		//First process the configuration passed to this adapter by forge:

		//Get the paths of the mbox files to process:
		String[] mboxFiles = config.get("MBOX");

		//Check to make sure at least one file was specified:
		if(mboxFiles == null)
			throw new AdapterException("You must specify at least one filename.");

		//Now that we have processed the configuration, we're ready to 

		//Loop over each of the files to parse:
		int nFiles = Array.getLength(mboxFiles);
		for(int n=0; n<nFiles; n++) {
			try{
				//Open the current file:
				BufferedReader reader
					= new BufferedReader(new FileReader(mboxFiles[n]));

				//process the current file:

				//The first line of each message should look like
				//"From <sender> <date> <more info>":
				String curFromLine = reader.readLine();
				
				if(curFromLine == null) {
					log.warning("mbox file '" + mboxFiles[n] + "' was empty.");
					continue; //Continue on to next file.
				}

				//Loop over the messages in the file:
				while(curFromLine != null) {
					//Report our progress:
					log.info("Processing message: " + curFromLine + "...");

					//Extract fields from the from line:

					Matcher matcher = fromLineRegex.matcher(curFromLine);
					
					if(!matcher.matches()) {
						log.warning("Invalid From line syntax in file '"
												+ mboxFiles[n] + "': " + curFromLine);
						break; //Abort this file.
					}

					String sender = matcher.group(1);
					String date = matcher.group(2);
					String senderInfo = matcher.group(3);

					//Create a new Record for this message and add the from line
					//fields as properties:
					Record record = new Record();
					
					record.add(new PVal(PROP_NAME_SENDER, sender));
					record.add(new PVal(PROP_NAME_DATE, date));
					
					if(!senderInfo.equals(""))
						record.add(new PVal(PROP_NAME_SENDER_INFO, senderInfo));

					processHeaders(reader, record); //process the message headers.

					//The rest of the message is the message body. This method will
					//read that in, add it to the record and return the From line of
					//the next message, if any:
					curFromLine = processBody(reader, record);

					handler.emit(record); //Emit the completed record.
				}

				reader.close(); //close the current file.
			}catch(IOException e) {
				//There was a problem processing the current file, but maybe the
				//others will work; we'll log an error and continue:
				log.severe("Error processing mbox file '" + mboxFiles[n] + "': "
									 + e.getMessage());
			}
		}
	}

	/***************************
	 * MBoxAdapter Implementation
	 ***************************/

	/**
	 * This method extracts all of the header fields from an mbox message
	 * and adds them to the specified record.
	 *
	 * @param reader BufferedReader to extract headers from
	 * @param record Record to add header properties to
	 *
	 * @throws IOException
	 */
	private void processHeaders(BufferedReader reader, Record record)
		throws IOException
	{
		//Loop until we reach a blank line, which indicates the end of the
		//headers, or we reach the end of the input stream:
		while(true) {
			String line = reader.readLine();

			if(line == null)
				break;

			if(line.equals(""))
				break;

			//Each header has the form "Name: value". Extract the name and
			//value from the current header:
			int colonPos = line.indexOf(':');
		
			if(colonPos == -1) {
				log.warning("Invalid message header format. Expected a colon in "
										+ "the line '" + line + "'");
				continue; //Move on to next header.
			}

			record.add(new PVal(line.substring(0, colonPos)
													, line.substring(colonPos + 1)));
		}
	}

	/**
	 * Beginning at the current position of the reader, this method reads
	 * in a message body until it reaches a blank line followed by a "From"
	 * line indicating the start of the next message, or the stream runs out
	 * of data. Once it is done reading in the body, it adds the body text
	 * to the specified record as a property, and returns the "From" line of
	 * the next message, if any.
	 *
	 * @param reader  Reader to read body from
	 * @param record  Record to add body to
	 * 
	 * @return  The "From" line of the next message in the reader stream, or
	 *          <code>null</code> if there are no more messages
	 *
	 * @throws IOException
	 */
	private String processBody(BufferedReader reader, Record record)
		throws IOException
	{
		String body = "";
		String fromLine = null;

		while(true) {
			String line = reader.readLine();

			if(line == null)
				break;

			if(line.equals("")) {
				fromLine = reader.readLine();
				
				if(fromLine == null)
					break;

				//If the line begins with "From " then it is a From line:
				if(fromLine.regionMatches(true, 0, "From ", 0, 5))
					break; //A new message was found.

				//not a from line...
				line += fromLine;
				fromLine = null;
			}

			body += line; //Append line to body.
		}

		//Add the body to the record:
		record.add(new PVal(PROP_NAME_BODY, body));
		return fromLine;
	}
}
