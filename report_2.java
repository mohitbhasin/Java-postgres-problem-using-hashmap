/*
Report 3
Prepared By MOHIT BHASIN.
CWID 10400111


 *About the Program
-This program will print number of quantity which are between that quater's average sales 
and maximum sales quantities.
-Programming language used is JAVA. 
-The only SQL command used is select * from sales.
-The output is produced in single scan of ResultSet.

 *How to run the program
-Make sure that JDK is installed on the machine and "sales" table is created in Postgres database.
-Run the following command in terminal to set CLASSPATH of Postgres driver for accessing Postgres Database
	export CLASSPATH=/home/user/java/postgresql-9.2-1003.jdbc4.jar:.
-Type "javac report_2.java" in terminal to compile he program.
-Type "java report_2" to run it.

 *DataStructure Used
-The DataStructure used is HashMap. HashMap is chosen because it is based on key-value relations
	which is easy to create and manage. HashMaps are also very fast.
-Arraylist is also used for storing Maximum quantities.

 *Algorithm of the Program
-A HashMap is created with key column containing "Customers" and value column containing a new HashMap.
-Another HashMap is created with customer,product and ArrayList for quantities.
-In both HashMap, key column contains "Product" and value column contains null initially.
-New HashMap Quarter is created with keys as Q1,Q2,Q3,Q4 and new average object with values 0,0.
-Now values of sales quantities are added in HashMAp quarter on the bases of months.
-Then the above hashMap is added to the original HashMap containing combination of customer & product.  
-Finally, calculate the averages and print the data.
 */

import java.sql.*;
import java.util.*;

public class report_2

{
	// global ResultSet declaration
	private static ResultSet rs = null;
	// Declaration of data structure used for average and maximumimum quantities
	// in this program i.e HashMap
	private static HashMap<String, HashMap<String, HashMap<String, Average>>> map = new HashMap<String, HashMap<String, HashMap<String, Average>>>();
	// Declaration of data structure used for storing all quantities in this
	// program i.e HashMap
	private static HashMap<String, HashMap<String, HashMap<String, ArrayList<Double>>>> map2 = new HashMap<String, HashMap<String, HashMap<String, ArrayList<Double>>>>();

	// executable method
	public static void main(String[] args) {
		String usr = "postgres";
		String pwd = "12345";
		String url = "jdbc:postgresql://localhost:5432/postgres";   
		
	
		
		
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		}

		catch (Exception e) {
			System.out.println("Fail loading Driver!");
			e.printStackTrace();
		}

		try {
			Connection conn = DriverManager.getConnection(url, usr, pwd);
			System.out.println("Success connecting server!");

			Statement stmt = conn.createStatement();
			// executing query
			rs = stmt.executeQuery("SELECT * FROM Sales");

			while (rs.next()) {
				// this condition checks whether customer is already added, if
				// not then new customer will be added
				if (!(map.containsKey(rs.getString("cust")))) {
					map.put(rs.getString("cust"),
							new HashMap<String, HashMap<String, Average>>());
					map2.put(
							rs.getString("cust"),
							new HashMap<String, HashMap<String, ArrayList<Double>>>());
				}
				// calling addProduct method passing parameter customer name
				addProd(rs.getString("cust"));
			}
			// print method is called for outputting data
			print_data();
			// if exception is thrown from above code then catch will catch the
			// exception and print stack trace of exception
		} catch (SQLException e) {
			System.out
					.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param cust
	 * @throws SQLException
	 *             method for adding Product for particular Customer it will
	 *             check if Customer already contains Product, if not then add
	 *             Product as key to HashMap and corresponding value i.e other
	 *             HashMap and call addQuarter method if Customer already
	 *             contains Product then it will directly call addQuarter method
	 */

	private static void addProd(String customer) throws SQLException {
		// this condition checks whether product is already added, if not then
		// new product will be added
		if (!(map.get(customer).containsKey(rs.getString("prod")))) {
			map.get(customer).put(rs.getString("prod"), null);
			map2.get(customer).put(rs.getString("prod"), null);
		}
		// calling function addQuarter with parameter customer name and product
		// name
		Quarter(customer, rs.getString("prod"));
	}

	/**
	 * 
	 * @param cust
	 * @param prod
	 * @throws SQLException
	 *             this method will add quantity to corresponding customer,
	 *             product and quarter i.e if month <=3 then add to quarter_1
	 *             and so on
	 */

	private static void Quarter(String customer, String product)
			throws SQLException {
		if (map.get(customer).get(product) == null) {
			HashMap<String, Average> quarters = new HashMap<String, Average>();
			quarters.put("Q1", new Average(0, 0.0));
			quarters.put("Q2", new Average(0, 0.0));
			quarters.put("Q3", new Average(0, 0.0));
			quarters.put("Q4", new Average(0, 0.0));
			map.get(customer).put(product, quarters);
			HashMap<String, ArrayList<Double>> quarters2 = new HashMap<String, ArrayList<Double>>();
			quarters2.put("Q1", new ArrayList<Double>());
			quarters2.put("Q2", new ArrayList<Double>());
			quarters2.put("Q3", new ArrayList<Double>());
			quarters2.put("Q4", new ArrayList<Double>());
			map2.get(customer).put(product, quarters2);
		}
		HashMap<String, Average> quarters = map.get(customer).get(product);
		if (rs.getInt("month") < 4) {
			Average comb = quarters.get("Q1");
			int count = comb.getCount();
			double quantity = comb.getQuantity();
			quantity = quantity + rs.getInt("quant");
			count = count + 1;
			comb.setCount(count);
			comb.setQuantity(quantity);
			quarters.put("Q1", comb);
			map2.get(customer).get(product).get("Q1")
					.add(rs.getInt("quant") + 0.0);
		} else if (rs.getInt("month") >= 7 && rs.getInt("month") < 10) {
			Average comb = quarters.get("Q3");
			int count = comb.getCount();
			double quantity = comb.getQuantity();
			quantity = quantity + rs.getInt("quant");
			count = count + 1;
			comb.setCount(count);
			comb.setQuantity(quantity);
			quarters.put("Q3", comb);
			map2.get(customer).get(product).get("Q3")
					.add(rs.getInt("quant") + 0.0);
		} else if (rs.getInt("month") >= 4 && rs.getInt("month") < 7) {
			Average comb = quarters.get("Q2");
			int count = comb.getCount();
			double quantity = comb.getQuantity();
			quantity = quantity + rs.getInt("quant");
			count = count + 1;
			comb.setCount(count);
			comb.setQuantity(quantity);
			quarters.put("Q2", comb);
			map2.get(customer).get(product).get("Q2")
					.add(rs.getInt("quant") + 0.0);
		} else {
			Average comb = quarters.get("Q4");
			int count = comb.getCount();
			double quantity = comb.getQuantity();
			quantity = quantity + rs.getInt("quant");
			count = count + 1;
			comb.setCount(count);
			comb.setQuantity(quantity);
			quarters.put("Q4", comb);
			map2.get(customer).get(product).get("Q4")
					.add(rs.getInt("quant") + 0.0);
		}
		map.get(customer).put(product, quarters);
	}

	/**
	 * method for printing HashMap (data) in required output using format method
	 * of String class for formating data in required output
	 */

	private static void print_data() {
		System.out.println("CUSTOMER" + " PRODUCT" + " QUARTER" + " BEFORE_TOT"
				+ " AFTER_TOT");
		System.out.println("========" + " =======" + " =======" + " =========="
				+ " =========");

		for (String customer : map.keySet()) {
			for (String product : map.get(customer).keySet()) {
				for (String quarter : map.get(customer).get(product).keySet()) {
					if (quarter.equals("Q1")) {
						int after = 0;
						double avg = map.get(customer).get(product).get("Q1")
								.getQuantity()
								/ map.get(customer).get(product).get("Q1")
										.getCount();
						double maximum = 0.0;
						for (double quant : map2.get(customer).get(product)
								.get("Q1")) {
							if (quant > maximum) {
								maximum = quant;
							}
						}
						for (double quant : map2.get(customer).get(product)
								.get("Q2")) {
							if (quant >= avg && quant <= maximum) {
								after++;
							}
						}
						System.out
								.printf("%-8s %-7s %-7s %-10s %9s %n",
										customer, product, quarter,
										"    <NULL>", after);

					} else if (quarter.equals("Q2")) {
						int before = 0;
						int after = 0;
						double avg = map.get(customer).get(product).get("Q2")
								.getQuantity()
								/ map.get(customer).get(product).get("Q2")
										.getCount();
						double maximum = 0.0;
						for (double quant : map2.get(customer).get(product)
								.get("Q2")) {
							if (quant > maximum) {
								maximum = quant;
							}
						}
						for (double quant : map2.get(customer).get(product)
								.get("Q1")) {
							if (quant >= avg && quant <= maximum) {
								before++;
							}
						}
						for (double quant : map2.get(customer).get(product)
								.get("Q3")) {
							if (quant >= avg && quant <= maximum) {
								after++;
							}
						}
						System.out.printf("%-8s %-7s %-7s %10s %9s %n",
								customer, product, quarter, before, after);
					} else if (quarter.equals("Q3")) {
						int before = 0;
						int after = 0;
						double avg = map.get(customer).get(product).get("Q3")
								.getQuantity()
								/ map.get(customer).get(product).get("Q3")
										.getCount();
						double maximum = 0.0;
						for (double d : map2.get(customer).get(product)
								.get("Q3")) {
							if (d > maximum) {
								maximum = d;
							}
						}
						for (double quant : map2.get(customer).get(product)
								.get("Q2")) {
							if (quant >= avg && quant <= maximum) {
								before++;
							}
						}
						for (double quant : map2.get(customer).get(product)
								.get("Q4")) {
							if (quant >= avg && quant <= maximum) {
								after++;
							}
						}
						System.out.printf("%-8s %-7s %-7s %10s %9s %n",
								customer, product, quarter, before, after);

					} else if (quarter.equals("Q4")) {
						int before = 0;
						double avg = map.get(customer).get(product).get("Q4")
								.getQuantity()
								/ map.get(customer).get(product).get("Q4")
										.getCount();
						double maximum = 0.0;
						for (double quant : map2.get(customer).get(product)
								.get("Q4")) {
							if (quant > maximum) {
								maximum = quant;
							}
						}
						for (double d : map2.get(customer).get(product)
								.get("Q3")) {
							if (d >= avg && d <= maximum) {
								before++;
							}
						}
						System.out.printf("%-8s %-7s %-7s %10s %9s %n",
								customer, product, quarter, before,
								"    <NULL>");
					}
				}
			}
		}

	}
}

class Average {
	int count = 0;
	int tot_quant = 0;
	int tot_others = 0;
	int count_others = 0;

	// the following functions are used to set and retrieve the values of count
	// and quantity.
	public void setCount(int value) {
		this.count = value;
	}

	public void setQuantity(int value) {
		this.tot_quant = value;
	}

	public int getCount() {
		return this.count;
	}

	public int getQuantity() {
		return this.tot_quant;
	}

	public int getCountOther() {
		return this.count_others;
	}

	public int getQuantityOther() {
		return this.tot_others;
	}
	
	
	public int accumulateCountOther(int count) {
		return this.count_others = this.count_others + count;
	}

	public int accumulateQuantityOther(int quant) {
		return this.tot_others = this.tot_others + quant;
	}
	
	
}
