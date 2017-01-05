/**
 *
 * @author Sahul P
 * Nikhil S
 */

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MinWindPerYearStation 
{
    public static class MinWindPerYearStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(0, 6);
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
                    String yearStation = year + "," + station;    
                    output.collect(new Text(yearStation), 
                                new DoubleWritable(windSpeed));
                }
            }
	}
    }
    
    public static class MinWindPerYearStationReducer extends MapReduceBase implements
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
        JobConf jobConfiguration = new JobConf(MinWindPerYearStation.class);
        jobConfiguration.setJobName("Finding minimum wind per year"
                + "per station");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MinWindPerYearStationMapper.class); 
        jobConfiguration.setReducerClass(MinWindPerYearStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }  
}
