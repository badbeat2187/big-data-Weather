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
public class MaxTempEachStation 
{
    public static class MaxTempEachStationMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String station = line.substring(0, 6);
            double temperature = 0;
            String temperatureValueRead = line.substring(102, 108).trim();
            
            // Ignoring the first value as its the heading
            if(!temperatureValueRead.equals("") && 
                    !temperatureValueRead.equals("MAX"))
            {
                    temperature = Double.parseDouble(temperatureValueRead);
                    if (temperature != 9999.9) 
                    {
                            output.collect(new Text(station), 
                                    new DoubleWritable(temperature));
                    }
            }
	}
    }
    
    public static class MaxTempEachStationReducer extends MapReduceBase implements
                            Reducer<Text, DoubleWritable, Text, DoubleWritable> 
    {
        public void reduce(Text key, Iterator<DoubleWritable> values, 
                OutputCollector<Text, DoubleWritable> output, 
                Reporter reporter) throws IOException 
        {
            double maxTemperature = 0; 
            while (values.hasNext()) 
            {
                Double currentValue = values.next().get();
                if(currentValue>maxTemperature)
                {
                    maxTemperature = currentValue;
                }            
            }

            output.collect(key, new DoubleWritable(maxTemperature)); 
        }
    }
    
    public static void main(String[] args) throws IOException 
    {      
        JobConf jobConfiguration = new JobConf(MaxTempEachStation.class);
        jobConfiguration.setJobName("Finding Maximum temperatre each station"
                + "for all years");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MaxTempEachStationMapper.class); 
        jobConfiguration.setReducerClass(MaxTempEachStationReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }
}
