import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageViews_By_User extends Reducer <Text, Text, Text, Text>
{
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		//read headers from the mappers and write to output
		String H1 = Users.getHead();
		String H2 = PageViews.getHead();
		
		context.write(new Text(H1), new Text(H2));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		//Reducer process
			String pageid = "";
			String age = "";
			
			for (Text t : values) 
			{ 
				String parts[] = t.toString().split("\t");
				if (parts[0].equals("userInfo")) 
				{
					age = parts[1];	
				} 
				else if (parts[0].equals("pageInfo")) 
				{
					pageid = parts[1];
				}
				
				if (pageid!= "" && age!= "")
					context.write(new Text(age), new Text(pageid));
			}
	}
}
