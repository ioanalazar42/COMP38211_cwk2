/** 
 *
 * Copyright (c) University of Manchester - All Rights Reserved
 * Unauthorised copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Kristian Epps <kristian@xepps.com>, August 28, 2013
 * 
 * RDF Inverted Index
 * 
 * This Map Reduce program should read in a set of RDF/XML documents and output
 * the data in the form:
 * 
 * {predicate, object]}, [subject1, subject2, ...] 
 * 
 * @author Kristian Epps
 * 
 */
package uk.ac.man.cs.comp38211.exercise;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.Property;

import uk.ac.man.cs.comp38211.io.array.ArrayListWritable;
import uk.ac.man.cs.comp38211.ir.StopAnalyser;
import uk.ac.man.cs.comp38211.util.XParser;

public class RDFInvertedIndex extends Configured implements Tool
{
    private static final Logger LOG = Logger
            .getLogger(RDFInvertedIndex.class);

    public static class Map extends 
    	Mapper<LongWritable, Text, Text, Text>
    {        
    	@SuppressWarnings("unused")
        private StopAnalyser stopAnalyser = new StopAnalyser();
    	
        protected Text document = new Text();
        
        // will contain the rdf triple that the current word is being mapped to
        protected Text rdfTriple = new Text();
        
        protected HashSet<String> relevantPredicates= new HashSet<String>(Arrays.asList(
        		"http://dbpedia.org/property/writer",
        		"http://dbpedia.org/property/director",
        		"http://dbpedia.org/property/airdate",
        		"http://dbpedia.org/property/commentary",
        		"http://dbpedia.org/property/episodeNo",
        		"http://dbpedia.org/property/couchGag",
        		"http://dbpedia.org/property/episodeName",
        		"http://dbpedia.org/property/guestStar",
        		"http://dbpedia.org/property/title",
        		"http://dbpedia.org/property/blackboard",
        		"http://purl.org/dc/terms/subject",
        		"http://xmlns.com/foaf/0.1/primaryTopic",
        		"http://dbpedia.org/property/showRunner"));
        
        // Takes a predicate, subject or object and extracts relevant words from it
        // and eliminates useless characters   
        private String extractWords(String initRDF) {
        	String res = new String(initRDF);
        	
        	// eliminate substring after #
        	if (res.contains("#")) {
        		res = res.substring(0, res.indexOf("#"));
        	}

        	// eliminate everything after ^^ (object type)
        	if (res.contains("^^")) {
        		res = res.substring(0, res.indexOf("^"));
        	}
        	
        	// eliminate language annotation (e.g. @en)
        	if (res.contains("@")) {
        		res = res.substring(0, res.indexOf("@"));
        	}
        	
        	// if URL then keep substring after /
        	if (res.contains("http")) {
        		res = res.substring(res.lastIndexOf("/") + 1);
        	}
        	
        	// replace underscore with space (e.g. Sara_Sloane -> Sara Sloane)
        	res = res.replaceAll("_", " ");
        	
        	// replace hyphen with space (e.g. 2001-12-02 -> 2001 12 02)
        	res = res.replaceAll("-", " ");
        			
        	// map certain non-alpha characters to the character they are encoding (e.g. %28 -> ( and %27 -> ')
        	res = res.replaceAll("%27", "\'");
        	res = res.replaceAll("%28", "(");
        	res = res.replaceAll("%29", ")'");
        	
        	// get rid of non-alphanumeric characters (keep spaces to be able to do tokenization)
        	res = res.replaceAll("[^A-Za-z0-9 ]", "");
        	
        	res = splitCamelCase(res);
        	
        	// transform everything to lower cases
        	return res.toLowerCase();  	
        }
        
        // turn camel case into space separated words
        // e.g. couchGag --> couch Gag
        static String splitCamelCase(String s) {
        	   return s.replaceAll(
        	      String.format("%s|%s|%s",
        	         "(?<=[A-Z])(?=[A-Z][a-z])",
        	         "(?<=[^A-Z])(?=[A-Z])",
        	         "(?<=[A-Za-z])(?=[^A-Za-z])"
        	      ),
        	      " "
        	   );
        	}
        
        private void emitWords(String words, Context context) throws IOException, InterruptedException {
        	String[] tokens = words.split(" ");
        	
        	for (String token : tokens) {
        		// if after tidying the RDF there is nothing left do not emit anything
        		if (token.equals(" ") || token.equals("")) {
        			continue;
        		}
        		
        		// get rid of stop words
        		if (StopAnalyser.isStopWord(token)) {
        			continue;
        		}
        		context.write(new Text(token), rdfTriple);
        	}
        }
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {            
            // This statement ensures we read a full rdf/xml document in
            // before we try to do anything else
            if(!value.toString().contains("</rdf:RDF>"))
            {    
                document.set(document.toString() + value.toString());
                return;
            }
            
            // We have to convert the text to a UTF-8 string. This must be
            // enforced or errors will be thrown. 
            String contents = document.toString() + value.toString();
            contents = new String(contents.getBytes(), "UTF-8");
            
            // The string must be cast to an inputstream for use with jena
            InputStream fullDocument = IOUtils.toInputStream(contents);
            document = new Text();
            
            // Create a model
            Model model = ModelFactory.createDefaultModel();
            
            try
            {
                model.read(fullDocument, null);
            
                StmtIterator iter = model.listStatements();
            
                // Iterate over all the triples, set and output them
                while(iter.hasNext())
                {
                	rdfTriple = new Text();
                	
                    Statement stmt      = iter.nextStatement();
                    Resource  subject   = stmt.getSubject();
                    Property  predicate = stmt.getPredicate();
                    RDFNode   object    = stmt.getObject();
                    
                    // if we don't care about this predicate, discard the whole statement
                    // some information may be irrelevant for our index as users would never search for it
                    if (!relevantPredicates.contains(predicate.toString())) {
                    	continue; // skip current statement
                    }
                    
                    rdfTriple.set(String.format("(%s, %s, %s)", subject.toString(), predicate.toString(), object.toString()));
                    
                    // extract plain words from subject, predicate and object
                    String wordsFromSubject = extractWords(subject.toString());
                    String wordsFromPredicate = extractWords(predicate.toString());
                    String wordsFromObject = extractWords(object.toString());
                    
                    emitWords(wordsFromSubject, context);
                    emitWords(wordsFromPredicate, context);
                    emitWords(wordsFromObject, context);
                }
            }
            catch(Exception e)
            {
                LOG.error(e);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, ArrayListWritable<Text>>

    {
       
        // This reducer turns an iterable into an ArrayListWritable, sorts it
        // and outputs it
        public void reduce(
                Text key,
                Iterable<Text> values,
                Context context) throws IOException, InterruptedException
        {
            ArrayListWritable<Text> postings = new ArrayListWritable<>();            
            ArrayList<String> rdfTriples = new ArrayList<String>();
            Iterator<Text> iter = values.iterator();
            
            
            while(iter.hasNext()) {
            	Text rdfTriple = iter.next();
            	
            	if (!rdfTriples.contains(rdfTriple.toString())) {
            		rdfTriples.add(rdfTriple.toString());
            		postings.add(new Text(rdfTriple));
            	}
            }
            
            Collections.sort(postings);
            
            context.write(key, postings);
        }
    }

    public RDFInvertedIndex()
    {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception
    {        
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline = null;
        CommandLineParser parser = new XParser(true);

        try
        {
            cmdline = parser.parse(options, args);
        }
        catch (ParseException exp)
        {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            System.err.println(cmdline);
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT))
        {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }      
        
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        Job RDFIndex = new Job(new Configuration());

        RDFIndex.setJobName("Inverted Index 1");
        RDFIndex.setJarByClass(RDFInvertedIndex.class);        
        RDFIndex.setMapperClass(Map.class);
        RDFIndex.setReducerClass(Reduce.class);
        
        RDFIndex.setMapOutputKeyClass(Text.class);
        RDFIndex.setMapOutputValueClass(Text.class);
        RDFIndex.setOutputKeyClass(Text.class);
        RDFIndex.setOutputValueClass(ArrayListWritable.class);  
        
        FileInputFormat.setInputPaths(RDFIndex, new Path(inputPath));
        FileOutputFormat.setOutputPath(RDFIndex, new Path(outputPath));

        long startTime = System.currentTimeMillis();
       
        RDFIndex.waitForCompletion(true);
        if(RDFIndex.isSuccessful())
            LOG.info("Job successful!");
        else
            LOG.info("Job failed.");
        
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new RDFInvertedIndex(), args);
    }
}
