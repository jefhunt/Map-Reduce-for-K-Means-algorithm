import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.StringTokenizer;

 public class KMeans {    
      static Point []centroid1=new Point[3];
      static Point []centroid2=new Point[3];
      static int execute_write=0;
     
     public static  void DefaultValInit (){
       float x,y;
         
        for(int i=0;i<3;i++)
       {
             x=new Float(Math.random()*3).floatValue();
             y=new Float(Math.random()*3).floatValue();
             centroid1[i]=new Point();
             centroid2[i]=new Point();
             Point temp=new Point();
             centroid2[i].set(x,y);
       }
	centroid2[0].set(2,2);
	centroid2[1].set(80,80);
	centroid2[2].set(-70,-70);
   }
    
     


 public static  class Point implements WritableComparable<Point> {

    private FloatWritable x,y;	

    public Point() {
	x = new FloatWritable();
	y = new FloatWritable();		
    }
	
    public void set ( float a, float b)
    {
	x.set(a);
	y.set(b);	
    }
	
    
    
    @Override
    public int  compareTo(Point p)
    {
        
        if (p.getx()+p.gety() >getx()+gety())
        {
            return 1;
        }
        else if (p.getx()+p.gety() <getx()+gety())
        {
            return 1;
        }
        else
            return 0;
        
    }
	
	
    public float getx() {
	return x.get();
    }

    public float gety() {
	return y.get();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        x.write(d);
        y.write(d);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        x.readFields(di);
        y.readFields(di);
        
    }

    public String toString()
    {
        return String.valueOf(getx()+","+String.valueOf(gety()));
    }

}

      public static   class KMeansMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Point> {
      //hadoop supported data types
      private  IntWritable one;
      private  Point point; 	
      public int findCentroid(Point p1)
      {
      		float max=new Float(distance(p1, centroid1[0]));
                int indexOfCentroid=0;
                for(int i=0;i<3;i++)
      		{
      			float distance=new Float(distance(p1, centroid1[i]));
      			if(distance<max)
      			{
      				max=distance;
      				indexOfCentroid=i;
      			}
      		}
      		return indexOfCentroid+1;
      }
      //map method that performs the tokenizer job and framing the initial key value pairs
      // after all lines are converted into key-value pairs, reducer is called.
      public void map(LongWritable key, Text value, OutputCollector<IntWritable, Point> output, Reporter reporter) throws IOException
      {
            
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
         
          //iterating through all the words available in that line and forming the key value pair
            while (tokenizer.hasMoreTokens())
            {
            	point = new Point();
            	String string[] =tokenizer.nextToken().split(","); 
            	point.set (new Float(string[0]) , new Float(string[1]));
               	one = new IntWritable(findCentroid(point));
               	//word.set();
               	//sending to output collector which inturn passes the same to reducer
                output.collect(one, point);
            }
       }
    }
     public static  class KMeansReducer extends MapReduceBase implements Reducer<IntWritable,Point,IntWritable,Point> {    

    @Override
    public void reduce(IntWritable key, Iterator<Point> value, OutputCollector<IntWritable, Point> output, Reporter rprtr) throws IOException {
        Point p;
        Text t;
        int count=0;
        Iterator<Point> value_dup=value;
        float sum_x=0,sum_y=0,out_x,out_y;
        
        while(value.hasNext())
        {
            p=value.next();
            sum_x+=p.getx();
            sum_y+=p.gety();
            count++;
            output.collect(key,p);
            
        }
        out_x=sum_x/count;
        out_y=sum_y/count;
        
        p=new Point();
        p.set(out_x,out_y);
        centroid2[key.get()-1]=p;
        
        t=new Text();
        
        
        
        

            
    }
}
     public static  void copyCentroid()
    {
        for(int i=0;i<3;i++)
        {
            centroid1[i]=centroid2[i];
        }
    }
     public static  boolean dist(){
                 int count = 0;
                 boolean finish = false;
                 double dis; 
                  
        	for(int i = 0; i < 3; i++) {
                 dis = distance(centroid1[i],centroid2[i]);       	
                 if(dis <=0.1 ) 
        	 count++;
               }
               if(count==3)
               {
        	 finish=true;
               }
            return finish; 	
     }
     public static  double distance(Point centroid, Point centroid2) {
          double distanc = 0;
           Float fx=new Float(centroid.getx());
           Double dx = new Double(fx.floatValue());
           Float fy=new Float(centroid.gety());
           Double dy = new Double(fy.floatValue());
           Float fx2=new Float(centroid2.getx());
           Double dx2 = new Double(fx2.floatValue());
           Float fy2=new Float(centroid2.gety());
           Double dy2 = new Double(fy2.floatValue());
          distanc = Math.sqrt(Math.pow((dx - dx2), 2) + Math.pow((dx - dx2), 2));
       return distanc;
       }
     
     public static  void run(String[] args) throws Exception
     {
                int i=0;
                long time1,time2 ;
	        time1=System.nanoTime();
		JobConf conf = new JobConf(KMeans.class);
		conf.setJobName("KMeans");
                conf.setNumMapTasks(3);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(KMeansMapper.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(KMeansReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
                
                conf.setMapOutputKeyClass(IntWritable.class);
                conf.setMapOutputValueClass(Point.class);
                

		FileInputFormat.setInputPaths(conf, new Path(args[0]));

                
                
  
                DefaultValInit();                  
                while(dist()==false) {
                    FileOutputFormat.setOutputPath(conf, new Path(args[1]+i));
		copyCentroid();
		JobClient.runJob(conf);  
                    i++;
        	}
		time2=System.nanoTime();
		System.out.println("\n\nExecution time is"+ (time2-time1)/Math.pow(10,9)  +"   sec ");
     }
     
     
     
	public static void main(String[] args) throws Exception {
	        KMeans.run(args);
	}
}
