### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
# WRITE YOUR CODE HERE
  regions_set=set()

  with open(data_filename,'r',encoding='utf-8') as f:
    next(f)
    for line in f:
      line=line.strip()
      cols=line.split('\t')
      if len(cols)<5:
        continue
      region=cols[4].strip()
      if region:
        regions_set.add(region)
  
  sorted_regions=sorted(list(regions_set))
  conn=create_connection(normalized_database_filename,delete_db=True)

  create_table_sql= """ 
  CREATE TABLE IF NOT EXISTS Region (
    RegionID INTEGER PRIMARY KEY,
    Region TEXT NOT NULL
  )
  """
  create_table(conn,create_table_sql,drop_table_name="Region")

  region_values=[(r,) for r in sorted_regions]

  with conn:
    conn.executemany("INSERT INTO Region (Region) VALUES (?)", region_values)
  conn.close()


def step2_create_region_to_regionid_dictionary(normalized_database_filename):
  pass
  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()

  cursor.execute("SELECT RegionID, Region FROM Region")
  rows=cursor.fetchall()

  region_dict={}
  for row in rows:
    region_id=row[0]
    region_name=row[1]
    region_dict[region_name]=region_id

  conn.close()
  return region_dict
    
    
# WRITE YOUR CODE HERE


def step3_create_country_table(data_filename,normalized_database_filename):
    
    # Inputs: Name of the data and normalized database filename
    # Output: None
    pass
    distinct_countries=set()
    with open(data_filename,'r') as file:
      next(file)
      for line in file:
        parts=line.strip().split('\t')
        country=parts[3].strip()
        region=parts[4].strip()
        distinct_countries.add((country,region))

    sorted_countries=sorted(distinct_countries,key=lambda x:x[0])

    conn=sqlite3.connect(normalized_database_filename)
    cursor=conn.cursor()

    create_table_sql= """ 
    CREATE TABLE Country(
      CountryID INTEGER PRIMARY KEY AUTOINCREMENT,
      CountryName TEXT NOT NULL,
      RegionID INTEGER NOT NULL,
      FOREIGN KEY (RegionID) REFERENCES Region (RegionID)

    );

    """
    create_table(conn,create_table_sql,drop_table_name="Country")

    rr=step2_create_region_to_regionid_dictionary(normalized_database_filename)

    insert_sql="INSERT INTO Country (CountryName,RegionID) VALUES(?,?)"

    country_rows=[]

    for country,region in sorted_countries:
      region_id=rr[region]
      country_rows.append((country,region_id))

    cursor.executemany(insert_sql,country_rows)

    conn.commit()
    conn.close()

# WRITE YOUR CODE HERE


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
  pass
  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()

  cursor.execute("SELECT CountryID,CountryName FROM Country")
  rows=cursor.fetchall()
  conn.close()

  ctoid={country: cid for cid, country in rows}
  return ctoid
# WRITE YOUR CODE HERE
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):
  pass
  ctocid=step4_create_country_to_countryid_dictionary(normalized_database_filename)
  customers=[]

  with open(data_filename,'r') as file:
    next(file)
    for line in file:
      parts=line.strip().split('\t')

      name=parts[0].strip()
      address=parts[1].strip()
      city=parts[2].strip()
      country=parts[3].strip()

      try:
        fname,lname=name.split(" ",1)
      except:
        continue

      try:
        country_id=ctocid[country]
      except KeyError:
        continue

      customers.append((fname,lname,address,city,country_id))
  
  customers.sort(key=lambda x: (x[0],x[1]))


     

  conn=sqlite3.connect(normalized_database_filename)
  
  create_table_sql=""" 
  CREATE TABLE Customer(
    CustomerID INTEGER PRIMARY KEY AUTOINCREMENT,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Address TEXT NOT NULL,
    City TEXT NOT NULL,
    CountryID INTEGER NOT NULL,
    FOREIGN KEY (CountryID) REFERENCES Country(CountryID)

  );
  
  """
  create_table(conn,create_table_sql,drop_table_name="Customer")
  insert_sql=""" 
  INSERT INTO Customer(FirstName,LastName,Address,City,CountryID)
  VALUES(?,?,?,?,?)
  """
  with conn:
    conn.executemany(insert_sql,customers)

  conn.close()
# WRITE YOUR CODE HERE


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
  pass
  
  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()

  cursor.execute("SELECT CustomerID,FirstName,LastName FROM Customer")
  rows=cursor.fetchall()

  nametocid={}
  for customer_id,fname,lname in rows:
    fullname=f"{fname} {lname}".strip()
    nametocid[fullname]=customer_id
  conn.close()
  return nametocid


# WRITE YOUR CODE HERE
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
  
  seen=set()
  distinct=[]
  with open(data_filename,'r',encoding='utf-8') as f:
    next(f)
    for line in f:
      parts=line.strip().split("\t")
      if len(parts)<8:
        continue

      categories=parts[6].split(";")
      description=parts[7].split(";")

      for cat,desc in zip(categories,description):
        cat=cat.strip()
        desc=desc.strip()

        if cat and cat not in seen:
          distinct.append((cat,desc))
          seen.add(cat)

  distinct.sort(key=lambda x: x[0])

  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()
  cursor.execute("DROP TABLE IF EXISTS ProductCategory")
  create_table_sql=""" 
  CREATE TABLE ProductCategory(
    ProductCategoryID INTEGER PRIMARY KEY AUTOINCREMENT,
    ProductCategory TEXT NOT NULL,
    ProductCategoryDescription TEXT NOT NULL
  );
  """
  cursor.execute(create_table_sql)
  insert_sql=""" 
  INSERT INTO ProductCategory(ProductCategory,ProductCategoryDescription) VALUES(?,?)

  """
  with conn:
    conn.executemany(insert_sql,distinct)
  conn.close()
# WRITE YOUR CODE HERE

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()

  cursor.execute("SELECT ProductCategory,ProductCategoryID FROM ProductCategory")
  rows=cursor.fetchall()
  conn.close()
  prodcatmap={row[0]: row[1] for row in rows}
  return prodcatmap
    


  pass
# WRITE YOUR CODE HERE
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
  prodcatdict=step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
  seen=set()
  distinct_products=[]
  with open(data_filename,'r',encoding='utf-8') as f:
    next(f)
    for line in f:
      parts=line.strip().split('\t')
      if len(parts)<9:
        continue

      product_names=parts[5].split(";")
      categories=parts[6].split(";")
      unitprstr=parts[8].split(";")
      for p,c,pr in zip(product_names,categories,unitprstr):
        pname=p.strip()
        catname=c.strip()
        try:
          unitprice=float(pr.strip())
          #categoryid=step8_create_productcategory_to_productcategoryid_dictionary.get(catname)
        except(ValueError,TypeError):
          continue
        
        categoryid=prodcatdict.get(catname)

        if categoryid is None:
          continue
        key=(pname,categoryid)
        if key not in seen:
          distinct_products.append((pname,unitprice,categoryid))
          seen.add(key)
  distinct_products.sort(key=lambda x:x[0])
  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()
  cursor.execute("DROP TABLE IF EXISTS Product")

  create_table_sql="""
  CREATE TABLE Product(
    ProductID INTEGER PRIMARY KEY AUTOINCREMENT,
    ProductName TEXT NOT NULL,
    ProductUnitPrice REAL NOT NULL,
    ProductCategoryID INTEGER NOT NULL,
    FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
  );
   """

  cursor.execute(create_table_sql)

  insert_sql="""
  INSERT INTO Product(ProductName,ProductUnitPrice, ProductCategoryID) VALUES(?,?,?)

   """
  with conn:
    conn.executemany(insert_sql,distinct_products)
  conn.close()
  
  pass
    
# WRITE YOUR CODE HERE


def step10_create_product_to_productid_dictionary(normalized_database_filename):
  pass
  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()
  cursor.execute("SELECT ProductID,ProductName FROM Product")
  rows=cursor.fetchall()
  conn.close()
  product_to_productid_dict={name: pid for pid,name in rows}
  return product_to_productid_dict

# WRITE YOUR CODE HERE
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
  import datetime
  prodtoprodid=step10_create_product_to_productid_dictionary(normalized_database_filename)
  custtocustid=step6_create_customer_to_customerid_dictionary(normalized_database_filename)

  orderrows=[]
  with open(data_filename,'r',encoding='utf-8') as f:
    next(f)
    for line in f:
      line_parts=line.strip().split("\t")
      if len(line_parts)<11:
        continue
      
      nameraw=line_parts[0].strip()
      try:
        #parts=name.strip().split(" ",1)
        first,last=nameraw.split(" ",1)
        fullnamekey=f"{first.strip()} {last.strip()}"

        customer_id=custtocustid.get(fullnamekey)
      except ValueError:
        continue
      if customer_id is None:
        continue
      
      product_names=line_parts[5].split(";")
      product_categories=line_parts[6].split(";")
      product_desc=line_parts[7].split(";")
      unitprice=line_parts[8].split(";")
      quantities=line_parts[9].split(";")
      dates=line_parts[10].split(";")

      for pname,qty,od in zip(product_names,quantities,dates):
        pname=pname.strip()
        qty=qty.strip()
        od=od.strip()

        prodid=prodtoprodid.get(pname)

        if prodid is None:
          continue

        

        try:
          qtyval=int(qty)

        except ValueError:
          continue

        try:
          orderdate=datetime.datetime.strptime(od,"%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
          continue
        orderrows.append((customer_id,prodid,orderdate,qtyval))

        
  conn=sqlite3.connect(normalized_database_filename)
  cursor=conn.cursor()
  cursor.execute("DROP TABLE IF EXISTS OrderDetail")

  create_table_sql=""" 
  CREATE TABLE OrderDetail(
    OrderID INTEGER PRIMARY KEY AUTOINCREMENT,
    CustomerID INTEGER NOT NULL,
    ProductID INTEGER NOT NULL,
    OrderDate TEXT NOT NULL,
    QuantityOrdered INTEGER NOT NULL,
    FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
  );
  
  """

  cursor.execute(create_table_sql)
  insert_sql=""" 
  INSERT INTO OrderDetail(CustomerID,ProductID,OrderDate,QuantityOrdered) VALUES(?,?,?,?)
  """
  with conn:
    conn.executemany(insert_sql,orderrows)

  conn.close()
  pass
# WRITE YOUR CODE HERE


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    pass
    from mini_project2 import step6_create_customer_to_customerid_dictionary
    cdict=step6_create_customer_to_customerid_dictionary('normalized.db')
    cid=cdict[CustomerName]

    sql_statement = f"""
    SELECT 
      Customer.FirstName || ' ' || Customer.LastName AS Name,
      Product.ProductName,
      OrderDetail.OrderDate,
      Product.ProductUnitPrice,
      OrderDetail.QuantityOrdered,
      ROUND(Product.ProductUnitPrice*OrderDetail.QuantityOrdered,2) AS Total
    FROM OrderDetail
    JOIN Customer ON OrderDetail.CustomerID=Customer.CustomerID
    JOIN Product ON OrderDetail.ProductID=Product.ProductID
    WHERE Customer.CustomerID={cid};
    """

# WRITE YOUR CODE HERE
    return sql_statement

def ex2(conn, CustomerName):
    from mini_project2 import step6_create_customer_to_customerid_dictionary
    cdict=step6_create_customer_to_customerid_dictionary('normalized.db')
    cid=cdict[CustomerName]
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    pass
    sql_statement = f"""
    SELECT Customer.FirstName || ' ' || Customer.LastName AS Name,
    ROUND(SUM(Product.ProductUnitPrice*OrderDetail.QuantityOrdered),2) AS Total
    FROM OrderDetail
    JOIN Customer ON OrderDetail.CustomerID=Customer.CustomerID
    JOIN Product ON OrderDetail.ProductID=Product.ProductID
    WHERE Customer.CustomerID={cid}
    GROUP BY Customer.FirstName,Customer.LastName;
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex3(conn):
    pass
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = """
    SELECT
    Customer.FirstName || ' ' || Customer.LastName AS Name,
    ROUND(SUM(Product.ProductUnitPrice*OrderDetail.QuantityOrdered),2) AS Total
    FROM OrderDetail
    JOIN Customer 
    ON OrderDetail.CustomerID=Customer.CustomerID
    JOIN Product
    ON OrderDetail.ProductID=Product.ProductID
    GROUP BY Customer.CustomerID
    ORDER BY Total DESC;
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex4(conn):
    pass
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = """
    SELECT Region.Region AS Region,
    ROUND(SUM(Product.ProductUnitPrice*OrderDetail.QuantityOrdered),2) AS Total
    FROM OrderDetail
    JOIN Customer ON OrderDetail.CustomerID=Customer.CustomerID
    JOIN Product ON OrderDetail.ProductID=Product.ProductID
    JOIN Country ON Customer.CountryID=Country.CountryID
    JOIN Region ON Country.RegionID=Region.RegionID
    GROUP BY Region.Region
    ORDER BY Total DESC;
    
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex5(conn):
    pass
    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 

    sql_statement = """
    SELECT 
      Country.CountryName as Country,
      ROUND(SUM(Product.ProductUnitPrice*OrderDetail.QuantityOrdered)) AS Total
    FROM OrderDetail
    JOIN Customer ON OrderDetail.CustomerID=Customer.CustomerID
    JOIN Product ON OrderDetail.ProductID=Product.ProductID
    JOIN Country ON Customer.CountryID=Country.CountryID
    GROUP BY Country.CountryName
    ORDER BY Total DESC;
        """

# WRITE YOUR CODE HERE
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region

    sql_statement = """
    SELECT 
      Region.Region AS Region,
      Country.CountryName as Country,
      ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) As CountryTotal,
      RANK() OVER (PARTITION BY Region.Region ORDER BY SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)DESC) As TotalRank
    FROM OrderDetail
    JOIN Customer ON OrderDetail.CustomerID=Customer.CustomerID
    JOIN Product ON OrderDetail.ProductID=Product.ProductID
    JOIN Country ON Customer.CountryID=Country.CountryID
    JOIN Region ON Country.RegionID = Region.RegionID
    GROUP BY Region.Region, Country.CountryName
    ORDER BY Region.Region ASC, TotalRank ASC;
    """

# WRITE YOUR CODE HERE
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, Total, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"

    sql_statement = """
    WITH RankedCountries AS(
      SELECT 
        Region.Region,
        Country.CountryName AS Country,
        ROUND(SUM(Product.ProductUnitPrice*OrderDetail.QuantityOrdered)) AS CountryTotal,
        RANK() OVER (PARTITION BY Region.Region ORDER BY SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)DESC) AS CountryRegionalRank
      FROM OrderDetail
      JOIN Customer ON OrderDetail.CustomerID=Customer.CustomerID
      JOIN Product ON OrderDetail.ProductID=Product.ProductID
      JOIN Country ON Customer.CountryID=Country.CountryID
      JOIN Region ON Country.RegionID=Region.RegionID
      GROUP BY Region.Region,Country.CountryName
    )
    SELECT Region,Country,CountryTotal,CountryRegionalRank
    FROM RankedCountries
    WHERE CountryRegionalRank=1
    ORDER BY Region ASC
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex8(conn):
    pass
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!

    sql_statement = """
    WITH CustomerSales AS(
      SELECT CASE 
      WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER)BETWEEN 1 AND 3 THEN 'Q1'
      WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 4 AND 6 THEN 'Q2'
      WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 7 AND 9 THEN 'Q3'
      ELSE 'Q4'
      END AS Quarter,
      CAST(SUBSTR(OrderDetail.OrderDate,1,4)AS INTEGER)AS Year,
      OrderDetail.CustomerID,
      ROUND(SUM(Product.ProductUnitPrice*OrderDetail.QuantityOrdered)) AS Total
      FROM OrderDetail
      JOIN Product ON OrderDetail.ProductID=Product.ProductID
      GROUP BY Quarter,Year,OrderDetail.CustomerID
    )
    SELECT Quarter,Year,CustomerID,Total
    FROM CustomerSales 
    ORDER BY Year
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex9(conn):
    pass
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()

    sql_statement = """
    WITH CustomerSales AS(
      SELECT CASE 
      WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER)BETWEEN 1 AND 3 THEN 'Q1'
      WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 4 AND 6 THEN 'Q2'
      WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 7 AND 9 THEN 'Q3'
      ELSE 'Q4'
      END AS Quarter,
      CAST(SUBSTR(OrderDetail.OrderDate,1,4)AS INTEGER)AS YEAR,
      OrderDetail.CustomerID,
      ROUND(SUM(Product.ProductUnitPrice*OrderDetail.QuantityOrdered)) AS Total
      FROM OrderDetail
      JOIN Product ON OrderDetail.ProductID=Product.ProductID
      GROUP BY Quarter,Year,OrderDetail.CustomerID),
      RankedSales AS(
        SELECT Quarter,Year,CustomerID,Total,
        RANK() OVER (PARTITION BY Quarter,Year ORDER BY Total DESC) AS CustomerRank
        FROM CustomerSales
      )
      SELECT Quarter,Year,CustomerID,Total,CustomerRank
      FROM RankedSales
      WHERE CustomerRank<=5
      ORDER BY Year
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex10(conn):
    pass
    # Rank the monthy sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total

    sql_statement = """
    WITH MonthlySales AS(
      SELECT
      CASE SUBSTR(OrderDetail.OrderDate,6,2)
      WHEN '01' THEN 'January'
      WHEN '02' THEN 'February'
      WHEN '03' THEN 'March'
      WHEN '04' THEN 'April'
      WHEN '05' THEN 'May'
      WHEN '06' THEN 'June'
      WHEN '07' THEN 'July'
      WHEN '08' THEN 'August'
      WHEN '09' THEN 'September'
      WHEN '10' THEN 'October'
      WHEN '11' THEN 'November'
      WHEN '12' THEN 'December'
      END AS Month,

      SUM(ROUND(Product.ProductUnitPrice*OrderDetail.QuantityOrdered)) AS Total
      FROM Product
      JOIN OrderDetail ON OrderDetail.ProductID=Product.ProductID
      GROUP BY Month


    )
    SELECT Month,Round(Total) AS Total, RANK() OVER (ORDER BY Total DESC) AS TotalRank
    FROM MonthlySales
    """

# WRITE YOUR CODE HERE
    return sql_statement

def ex11(conn):
    pass
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag
    sql_statement = """
    WITH CustomerOrders AS(
      SELECT
      Customer.CustomerID,Customer.FirstName,Customer.LastName,Country.CountryName AS Country,OrderDetail.OrderDate,
      LAG(OrderDetail.OrderDate) OVER (PARTITION BY Customer.CustomerID ORDER BY OrderDetail.OrderDate) AS PreviousOrderDate
      FROM OrderDetail
      JOIN Customer ON OrderDetail.CustomerID=Customer.CustomerID
      JOIN Country ON Customer.CountryID=Country.CountryID

    ),
    DaysBW AS(
      SELECT CustomerID,FirstName,LastName,Country,OrderDate,PreviousOrderDate,
      JULIANDAY(OrderDate)-JULIANDAY(PreviousOrderDate) AS DaysWithoutOrder
      FROM CustomerOrders
      WHERE PreviousOrderDate IS NOT NULL
    ),
    MaxDays AS(
      SELECT CustomerID,MAX(DaysWithoutOrder) AS MaxDaysWithoutOrder FROM DaysBW GROUP BY CustomerID

    )
    SELECT DaysBW.CustomerID,DaysBW.FirstName,DaysBW.LastName,DaysBW.Country,DaysBW.OrderDate,DaysBW.PreviousOrderDate,MaxDays.MaxDaysWithoutOrder
    FROM DaysBW
    JOIN MaxDays ON DaysBW.CustomerID=MaxDays.CustomerID AND DaysBW.DaysWithoutOrder=MaxDays.MaxDaysWithoutOrder
    WHERE DaysBW.OrderDate=(
      SELECT MIN(DB2.OrderDate)
      FROM DaysBW DB2
      JOIN MaxDays M2 ON DB2.CustomerID=M2.CustomerID AND DB2.DaysWithoutOrder=M2.MaxDaysWithoutOrder
      WHERE DB2.CustomerID=DaysBW.CustomerID
    )
    ORDER BY MaxDays.MaxDaysWithoutOrder DESC, DaysBW.CustomerID DESC
    """
# WRITE YOUR CODE HERE
    return sql_statement