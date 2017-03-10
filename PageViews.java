import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PageViews extends Mapper <Object, Text, Text, Text>
{
	static String head3;
	
	public static String getHead()
	{
		return head3;
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		//Mapper for Page_Views table
		
		System.out.println("-----------------------------------This is pageViews mapper process--------------------------------------------------");
		String record = value.toString();
		String[] parts = record.split("\t");
		String strKey = key.toString();
		
		//If header then store in static variable else write in buffer for the reducer
		if (Integer.parseInt(strKey) == 0)
		{
			head3= parts[0];
		}
		else
		{
			context.write(new Text(parts[1]), new Text("pageInfo\t" + parts[0]));
		}
	
	}
}

