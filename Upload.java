/*
Derby Connection URL:
jdbc:derby://localhost:1527/populations;create=true;user=javauser;password=javauser

ij>
CREATE TABLE data (
	zip_code CHAR(5) NOT NULL PRIMARY KEY,
	population INTEGER DEFAULT 0 
);

Compile: javac Upload.java
Execute: java -jar ".;%JAVA_HOME%\db\lib\derbyclient.jar" Upload &lt;path to zipcodes.csv&gt;

Answer these questions by executing queries against this populated database.
Try to use JDBC to issue the queries and display the results.

What zip code has the highest population?
How many zip codes have 0 population?
What is the total population across all area codes?
What is the approximate population of Austin (all the zip codes starting with 787)?
What zip codes, in numerical order, have 1 resident?
What is the average number of residents in any US zip code?
How many zip codes are there in the .csv file?

*/
import java.io.*;
import java.sql.*;
import java.util.*;

public class Upload {
	public static void main(String[] args) {
		// path to the zipcodes.csv file is a command-line arg
		if (args.length != 1) {
			System.out.println("Usage: java Upload <csv-file>");
			return;
		}
		final String url = "jdbc:derby://localhost:1527/populations";
		final String user = "javauser";
		final String pass = "javauser";
		
		final Map<String, Integer> dataSet = new HashMap<>();
		
		long start = System.nanoTime(); // let's track how long this takes
		try (BufferedReader br = new BufferedReader(new FileReader(args[0]));) {
			String line;
			int lineno = 0;
			br.readLine();
			while ((line = br.readLine()) != null) {
				if (lineno % 1000 == 0)
					System.out.print(".");
				lineno++;
				String[] parts = line.split(",");
				String zip = parts[0];
				if (! zip.matches("\\d{5}"))
					throw new IOException(
						String.format(
							"Zip code invalid (%s) on line %d\n", zip, lineno));
				int pop = Integer.parseInt(parts[1]);
				if (pop < 0)
					throw new IOException(
						String.format(
							"Population less than zero (%d) on line %d\n", pop, lineno));
				if (dataSet.keySet().contains(zip))
					pop += dataSet.get(zip);
				else dataSet.put(zip, pop);
			}
		}
		catch (IOException ioe) {
			System.out.println(ioe.getMessage());
			return;
		}
		
		try (
			Connection conn = DriverManager.getConnection(url, user, pass);
			Statement stat = conn.createStatement();
		) {
			for (String zip : dataSet.keySet()) {
				String sql = String.format(
					"INSERT INTO data VALUES ('%s', %d)", zip, dataSet.get(zip));
				stat.executeUpdate(sql);
			}
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		long end = System.nanoTime();
		System.out.printf("Completed in %f seconds\n", (end-start) / 1000000000.0);
	}
}








