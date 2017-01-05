import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 *
 * @author Sahul P
 * Nikhil S
 */
public class MinWindPerYear 
{
    public static class MinWindPerYearMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String year = line.substring(14, 18);
            double windSpeed = 0;
            String windSpeedValueRead = line.substring(78, 83).trim();
            
            // Ignoring the first value as its the heading
            if(!windSpeedValueRead.equals("") && 
                    !windSpeedValueRead.equals("WDSP"))
            {
                    windSpeed = Double.parseDouble(windSpeedValueRead);
                    if (windSpeed != 999.9) 
                    {
                            output.collect(new Text(year), 
                                    new DoubleWritable(windSpeed));
                    }
            }
	}
    }
    
    public static class MinWindPerYearReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double minWindSpeed = Integer.MAX_VALUE; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue<minWindSpeed)
                {
                    minWindSpeed = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(minWindSpeed)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(MinWindPerYear.class);
        jobConfiguration.setJobName("Finding minimum wind per year");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MinWindPerYearMapper.class); 
        jobConfiguration.setReducerClass(MinWindPerYearReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }
}
