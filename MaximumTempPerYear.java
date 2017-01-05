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
public class MaximumTempPerYear 
{
    public static class MaximumTempPerYearMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
	public void map(LongWritable key, Text value,OutputCollector<Text, 
                DoubleWritable> output, Reporter reporter) throws IOException 
	{
            String line = value.toString();
            String year = line.substring(14, 18);
            double temperature = 0;
            String temperatureValueRead = line.substring(102, 108).trim();
            
            // Ignoring the first value as its the heading
            if(!temperatureValueRead.equals("") && 
                    !temperatureValueRead.equals("MAX"))
            {
                    temperature = Double.parseDouble(temperatureValueRead);
                    if (temperature != 9999.9) 
                    {
                            output.collect(new Text(year), 
                                    new DoubleWritable(temperature));
                    }
            }
	}
    }
    
    public static class MaximumTempPerYearReducer extends MapReduceBase implements
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
        JobConf jobConfiguration = new JobConf(MaximumTempPerYear.class);
        jobConfiguration.setJobName("Finding Maximum temperatre per year");
        
        FileInputFormat.addInputPath(jobConfiguration, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConfiguration, new Path(args[1]));
        
        jobConfiguration.setMapperClass(MaximumTempPerYearMapper.class); 
        jobConfiguration.setReducerClass(MaximumTempPerYearReducer.class);
        jobConfiguration.setOutputKeyClass(Text.class); 
        jobConfiguration.setOutputValueClass(DoubleWritable.class);
        
        JobClient.runJob(jobConfiguration); 
    }    
}
