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
public class MaxWindPerYear 
{
    public static class MaxWindPerYearMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String year = line.substring(14, 18);
            double windSpeed = 0;
            String windSpeedValueRead = line.substring(88, 93).trim();
            
            // Ignoring the first value as its the heading
            if(!windSpeedValueRead.equals("") && 
                    !windSpeedValueRead.equals("MXSPD"))
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
    
    public static class MaxWindPerYearReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double maxWindSpeed = 0; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue>maxWindSpeed)
                {
                    maxWindSpeed = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(maxWindSpeed)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(MaxWindPerYear.class);
        jobConfiguration.setJobName("Finding maximum wind per year");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MaxWindPerYearMapper.class); 
        jobConfiguration.setReducerClass(MaxWindPerYearReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }
}
