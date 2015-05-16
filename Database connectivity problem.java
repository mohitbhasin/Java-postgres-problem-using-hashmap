/*
Prepared By MOHIT BHASIN.

*About the Program
-This program will print average sales quantity of each combination of customer and product
 for NY, NJ, CT separately.
-Programming language used is JAVA. 
-The only SQL command used is select * from sales.
-The output is produced in single scan of ResultSet.

*How to run the program
-Make sure that JDK is installed on the machine and "sales" table is created in Postgres database.
-Run the following command in terminal to set CLASSPATH of Postgres driver for accessing Postgres Database
	export CLASSPATH=/home/user/java/postgresql-9.2-1003.jdbc4.jar:.
-Type "javac AvgSales.java" in terminal to compile he program.
-Type "java AvegSales" to run it.

*DataStructure Used
-The DataStructure used is HashMap. HashMap is chosen because it is based on key-value relations
	which is easy to create and manage. HashMaps are also very fast.

*Algorithm of the Program
-A HashMap is created with key column containing "Customers" and value column containing a new HashMap.
-In the new HashMap, key column contains "Product" and value column contains another HashMap.
-This third HashMap has "states" in the key and class "Average" in value.
-First, the result of the Select query is stored in the resultSet.
-While traversing ResultSet if State is "NY","NJ"or"CT",add customer if it is not in the HashMap.
-Now call "addProd" function with customer as the parameter, and repeat above step for product.
-Call "addState" function with customer and product as parameters.
*/

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


public class b 
{
//HashMap "map" will store combination of "customer","product" with different "states" and class "Average" 
	public	static HashMap<String, HashMap<String, HashMap<String,Average>>> map = new 
			HashMap<String, HashMap<String, HashMap<String,Average>>>();
	static ResultSet rs;
	public static void main(String args[])
	{
			String usr ="postgres";
			String pwd ="12345";
			String url ="jdbc:postgresql://localhost:5432/postgres";
			//Loading Postgres driver to establish connection with postgres database
			try 
			{
				Class.forName("org.postgresql.Driver");
				System.out.println("Success loading Driver!");
			} 
			//If any exception occurs in loading driver program will print the below String.
			catch(Exception e) 
			{
				System.out.println("Fail loading Driver!");
				e.printStackTrace();
			}
			//Establishing connection to the database using URL,userID and Password
			try
			{
			Connection conn = DriverManager.getConnection(url, usr, pwd);
			System.out.println("Success connecting server!");
			//Executing the Select Query and putting its result in ResultSet.
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM sales");
			while(rs.next())
			{
				//This Condition will check for NY,NJ and CT states
				if (rs.getString("state").equals("NJ") || 
					rs.getString("state").equals("NY") || 
					rs.getString("state").equals("CT"))
				{
				//If States are either NY,NJ or CT, this condition will add customer to the key column
				//	of HashMap if they don't exist.
				//	After adding customer new HashMap will be created for adding product.
				if(!(map.containsKey(rs.getString("cust"))))
						{
						map.put(rs.getString("cust"), new HashMap<String,HashMap<String,Average>>());
						}
				//Function addProd is called to add products with customer as the argument
				addProd(rs.getString("cust"));
			}
			}
			//Finally print_data() will print the table with customer,product and state wise average of product   
			conn.close();
			print_data();
			}
			//If any exception occurs in above try clause, it will print the following string. 
			catch(SQLException e) 
			{
				System.out.println("Connection URL or username or password errors!");
				e.printStackTrace();
			}
			}
	
//addProd() will accept customer as argument. If combination of customer and product does not exist in HashMap
// it will put product in the map and create new HashMap for states and class average. 	
public static void addProd(String customer) throws SQLException
{
	if(!(map.get(customer).containsKey(rs.getString("prod"))))
	{
	map.get(customer).put(rs.getString("prod"), new HashMap<String,Average>());
	}
//addState() is called with customer and product as arguments.	
	addState(customer,rs.getString("prod"));
}
//first, addState() will check for state, if state already exist then object of class Average will
// will update total count and total quantity by calling getCount() and getQuantity()
public static void addState(String customer,String product) throws SQLException
{
	if(map.get(customer).get(product).containsKey(rs.getString("state")))
	{
	Average comb=	map.get(customer).get(product).get(rs.getString("state"));
	comb.setCount(comb.getCount() + 1);
	comb.setQuantity(comb.getQuantity() + rs.getInt("quant"));
	map.get(customer).get(product).put(rs.getString("state"), comb);
	}
	else
	//If the state does not exist for the combination of customer and product then new state will be
	// inserted with total count set to "1" and its quantity.	
	{
		map.get(customer).get(product).put(rs.getString("state"), new Average(1,rs.getInt("quant")));	
	}
}
//print_data() will print the entire HashMap 
public static void print_data()
{
	System.out.println("CUSTOMER"+" PRODUCT"+"      NY_AVG"+"     NJ_AVG"+"     CT_AVG");
	System.out.println("========"+" ======="+"     ======="+"    ======="+"    =======");
	//For ever customer 
	for(String customer: map.keySet())
	{
		for(String product: map.get(customer).keySet())
		{
			double ny=0.0;
			double nj= 0.0;
			double ct= 0.0;
			for(String state:map.get(customer).get(product).keySet())
			{
				Average comb=map.get(customer).get(product).get(state);
				if(state.equals("NY"))
					ny=comb.getQuantity()/comb.getCount();
				if(state.equals("NJ"))
					nj=comb.getQuantity()/comb.getCount();
				if(state.equals("CT"))
					ct=comb.getQuantity()/comb.getCount();
			}
			System.out.printf("%-8s %-8s %10.2f %10.2f %10.2f %n",customer,product,ny,nj,ct);
		}
	}
}

}
//class Average will is use to initialize count and tot_quant variables.
class Average {
	int count;
	double tot_quant;
//This is default constructor. It will be called automatically
//	whenever object is created and will set values of count and tot_quant to "0".
	public Average() {
		this.count = 0;
		this.tot_quant = 0.0;
	} 
//This function will set the new values.	
	public Average(int count, double quant) {
		this.count = count;
		this.tot_quant = quant;
	} 
//the following functions are used to set and retrieve the values of count and quantity.
	public void setCount(int value) {
		this.count = value;
	}
	public void setQuantity(double value) {
		this.tot_quant = value;
	}
	public int getCount() {
		return this.count;
	} 
	public double getQuantity() {
		return this.tot_quant;
	}
}
