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

public class MinRainPerYearStation 
{
    public static class MinRainPerYearStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(0, 6);
            String year = line.substring(14, 18);
            double precipitation = 0;
            String precipitationValueRead = line.substring(118, 123).trim();
            
            // Ignoring the first value as its the heading
            if(!precipitationValueRead.equals("") && 
                    !precipitationValueRead.equals("PRCP"))
            {
                precipitation = Double.parseDouble(precipitationValueRead);
                if (precipitation != 99.99) 
                {
                    String yearStation = year + "," + station;    
                    output.collect(new Text(yearStation), 
                                new DoubleWritable(precipitation));
                }
            }
	}
    }
    
    public static class MinRainPerYearStationReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double minRain = Integer.MAX_VALUE; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue<minRain)
                {
                    minRain = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(minRain)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(MinRainPerYearStation.class);
        jobConfiguration.setJobName("Finding minimum rain per year"
                + "per station");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MinRainPerYearStationMapper.class); 
        jobConfiguration.setReducerClass(MinRainPerYearStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }  
}
