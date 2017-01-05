/**
 *
 * @author Sahul 
 * Nikhil S
 */

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MinWindEachStation 
{
    public static class MinWindEachStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(0, 6);
            double windSpeed = 0;
            String windSpeedValueRead = line.substring(78, 83).trim();
            
            // Ignoring the first value as its the heading
            if(!windSpeedValueRead.equals("") && 
                    !windSpeedValueRead.equals("WDSP"))
            {
                windSpeed = Double.parseDouble(windSpeedValueRead);
                if (windSpeed != 999.9) 
                {
                    output.collect(new Text(station), 
                                new DoubleWritable(windSpeed));
                }
            }
	}
    }
    
    public static class MinWindEachStationReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double minWind = Integer.MAX_VALUE; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue<minWind)
                {
                    minWind = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(minWind)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(MinWindEachStation.class);
        jobConfiguration.setJobName("Finding minimum wind each station");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MinWindEachStationMapper.class); 
        jobConfiguration.setReducerClass(MinWindEachStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }  
}
