import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Queue;


public class Schema2 {

//	CREATE TABLE Employee(Fname CHAR(20), Minit CHAR(10), Lname CHAR(20), ssn INT PRIMARY KEY, Bdate date, address CHAR(20), sex CHARACTER(1), salary INT, Super_snn INT REFERENCES Employee(ssn), dno INT);

    public static long insertEmployee(String Fname, String Minit, String Lname, int ssn, Date Bdate, String address, String sex, int salary, int superSSN, int dno, Connection conn) {
        String SQL = "INSERT INTO Employee(Fname,Minit,Lname,ssn,Bdate,address,sex,salary,Super_snn,dno) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?);";

        long id = 0;
        try {
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(1, Fname);
            pstmt.setString(2, Minit);
            pstmt.setString(3, Lname);
            pstmt.setInt(4, ssn);
            pstmt.setDate(5, Bdate);
            pstmt.setString(6, address);
            pstmt.setString(7, sex);
            pstmt.setInt(8, salary);
            pstmt.setInt(9, superSSN);
            pstmt.setInt(10, dno);

            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(4);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }
//	 CREATE TABLE Department(Dname CHAR(20), Dnumber INT PRIMARY KEY, Mgr_snn int REFERENCES employee, Mgr_start_date date );

    public static long insertDepartment(String Dname, int Dnumber, int MgrSSN, Date startDate, Connection conn) {
        String SQL = "INSERT INTO Department(Dname,Dnumber,Mgr_snn,Mgr_start_date) "
                + "VALUES(?,?,?,?);";

        long id = 0;
        try {
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(1, Dname);
            pstmt.setInt(2, Dnumber);
            pstmt.setInt(3, MgrSSN);
            pstmt.setDate(4, startDate);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(2);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }

    //	 CREATE TABLE Dept_locations(Dnumber integer REFERENCES Department, Dlocation CHAR(20), PRIMARY KEY(Dnumber,Dlocation));
    public static long insertDeptLocations(int Dnumber, String Dlocation, Connection conn) {
        String SQL = "INSERT INTO Dept_locations(Dnumber,Dlocation) "
                + "VALUES(?,?);";

        long id = 0;
        try {
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(2, Dlocation);
            pstmt.setInt(1, Dnumber);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(1);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }

    //	 CREATE TABLE Project(Pname CHAR(20), Pnumber INT PRIMARY KEY, Plocation CHAR(50), Dnumber INT REFERENCES Department);
    public static long insertProject(String Pname, int Pnumber, String pLocation, int Dnumber, Connection conn) {
        String SQL = "INSERT INTO Project(Pname,Pnumber,Plocation,Dnumber) "
                + "VALUES(?,?,?,?);";

        long id = 0;
        try {
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(1, Pname);
            pstmt.setInt(2, Pnumber);
            pstmt.setString(3, pLocation);
            pstmt.setInt(4, Dnumber);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(2);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }
//	 CREATE TABLE Works_on(Essn int REFERENCES Employee, Pno int REFERENCES Project, Hours int, PRIMARY KEY(Essn,Pno));

    public static long insertWorksOn(int Essn, int pNo, int hours, Connection conn) {
        String SQL = "INSERT INTO Works_on(Essn,Pno,Hours) "
                + "VALUES(?,?,?);";

        long id = 0;
        try {
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setInt(2, pNo);
            pstmt.setInt(1, Essn);
            pstmt.setInt(3, hours);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(1);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }

    //	 CREATE TABLE Dependent(Essn INT REFERENCES Employee, Dependent_name CHAR(20), sex CHARACTER(1), Bdate date, Relationship CHAR(20), PRIMARY KEY(Essn, Dependent_name));
    public static long insertDependent(int Essn, String dependentName, String sex, Date Bdate, String relationship, Connection conn) {
        String SQL = "INSERT INTO Dependent(Essn,Dependent_name,sex,Bdate,Relationship) "
                + "VALUES(?,?,?,?,?);";

        long id = 0;
        try {
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setInt(1, Essn);
            pstmt.setString(2, dependentName);
            pstmt.setString(3, sex);
            pstmt.setDate(4, Bdate);
            pstmt.setString(5, relationship);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(1);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }

    /////////////////////////////////////////////// Data Population Methods //////////////////////////////////////////////////////////////
    public static Queue<Integer> Dnos = new LinkedList<>();
    public static Queue<Integer> Pnos = new LinkedList<>();
    public static Queue<Integer> Essns = new LinkedList<>();

    @SuppressWarnings("deprecation")
    public static void populateEmployee(Connection conn) {
        String result = "M";
        for (int i = 1; i < 15401; i++) {
            int randYear = ((int) Math.round(Math.random() * (101 - 56)) + 56); //random year between 1956 and 2000
            int randMonth = (int) (Math.random() * (12 - 1)) + 1; // random month between 1 and 12
            int randDay;
            if (randMonth == 2)
                randDay = (int) (Math.random() * (29 - 1)) + 1; //random day between 1 and 29
            randDay = (int) (Math.random() * (31 - 1)) + 1; //random day between 1 and 31
            //  int randSalary1 = (int) (Math.random() * (100000 - 70000)) + 70000; //random salary between 30000 and 100000
            //int randDepartment1 = (int) (Math.random() * (150 - 6)) + 6; //random department between 1 and 150
            int randSalary2 = (int) (Math.random() * (34999 - 7000)) + 7000; //random salary between 30000 and 100000
            int randDepartment2 = (int) (Math.random() * (50 - 1)) + 1; //random department between 1 and 150

            if (i > 600)
                if(i%2==1)
                    result = "F";
                else
                    result = "M";
            if (i == 1) {
                insertEmployee("Name" + i, "M" + i, "employee" + i, i,
                        new Date(randYear, randMonth, randDay),
                        "address" + i, result, 35000, i,
                        5, conn);
                i++;
            }
//
//            while(i<602) {
//                insertEmployee("Name" + i, "M" + i, "employee" + i, i,
//                        new Date(randYear, randMonth, randDay),
//                        "address" + i, result, randSalary1, i,
//                        randDepartment1, conn);
//                i++;
//            }

            if (insertEmployee("Name" + i, "M" + i, "employee" + i, i,
                    new Date(randYear, randMonth, randDay),
                    "address" + i, result, randSalary2, i,
                    randDepartment2, conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }
//        int i = 16000;
//        insertEmployee("Employee" + i, "M" + i, "Employee" + i, i,
//                new Date(randYear,randMonth,randDay),
//                "address" + i, result,randSalary, i,
//                randDepartment, conn);
//        i++;
//        insertEmployee("Employee" + i, "M" + i, "Employee" + i, i,
//                new Date(((int) Math.random()*(2001-1960)), ((int) Math.random()*(12-1)), ((int) Math.random()*(31-1))),
//                "address" + i, "M", i, i, 1, conn);

    @SuppressWarnings("deprecation")
    public static void insert_Q6(Connection conn) {
        int dep=50;
        for(int i=15401;i<16099;i++) {
            dep++;
            for(int j=0;j<6;j++) {
                int randYear = ((int) Math.round(Math.random() * (101 - 56)) + 56); //random year between 1956 and 2000
                int randMonth = (int) (Math.random() * (12 - 1)) + 1; // random month between 1 and 12
                int randDay;
                if (randMonth == 2)
                    randDay = (int) (Math.random() * (29 - 1)) + 1; //random day between 1 and 29
                randDay = (int) (Math.random() * (31 - 1)) + 1; //random day between 1 and 31
                int randSalary1 = (int) (Math.random() * (69000 - 40001)) + 40001; //random salary between 30000 and 100000
                i++;
                insertEmployee("Name" + i, "M" + i, "employee" + i, i,
                        new Date(randYear, randMonth, randDay),
                        "address" + i, "M", randSalary1, i,
                        dep, conn);
            }


        }

    }


    @SuppressWarnings("deprecation")
    public static void populateDepartment(Connection conn) {
        for (int i = 1; i < 151; i++) {
            int randYear = ((int) Math.round(Math.random() * (122 - 77)) + 77); //random year between 1977 and 2022
            int randMonth = (int) (Math.random() * (12 - 1)) + 1; //random month between 1 and 12
            int randDay;
            if (randMonth == 2)
                randDay = (int) (Math.random() * (29 - 1)) + 1; //random day between 1 and 29
            randDay = (int) (Math.random() * (31 - 1)) + 1; //random day between 1 and 31
            int randMg_ssn = (int) (Math.random() * (15421 - 1)) + 1; //random manager ssn between 1 and 16000
            if(i==1){
                insertDepartment("Department" + i,i,1, new Date(randYear, randMonth, randDay), conn);
                i++;
            }
            if (insertDepartment("Department" + i, i, randMg_ssn,
                    new Date(randYear, randMonth, randDay), conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    public static void populateDeptLocations(Connection conn) {
        for (int i = 1; i < 151; i++) {
            if (insertDeptLocations(i, "Location" + i, conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    public static void populateProject(Connection conn) {
        for (int i = 1; i < 9201; i++) {
            int randLoc = (int) (Math.random() * (150 - 1)) + 1; //random location between 1 and 150
            int randDept = (int) (Math.random() * (150 - 2)) + 2; //random department between 1 and 150
            if (i == 1) {
                insertProject("Project" + i, i, "Location" + randLoc, 5, conn);
                i++;
            } if (insertProject("Project" + i, i, "Location" + randLoc, randDept, conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    public static void populateWorksOn(Connection conn) {
        for (int i = 1; i < 1000; i++) {
            int randProj = (int) (Math.random() * (9200 - 2)) + 2; //random project between 1 and 9200
            int randHour = (int) (Math.random() * (150 - 24)) + 24; //random hour between 24 and 150
            while(i<601) {
                insertWorksOn(1, i, randHour, conn);
                i++;
            }
            if (insertWorksOn(i, randProj, randHour, conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    @SuppressWarnings("deprecation")
    public static void populateDependent(Connection conn) {
        for (int i = 1; i < 601; i++) {
            int randYear = ((int) Math.round(Math.random() * (122 - 100)) + 100); //random year between 2000 and 2022
            int randMonth = (int) (Math.random() * (12 - 1)) + 1; //random month between 1 and 12
            int randDay;
            if (randMonth == 2)
                randDay = (int) (Math.random() * (29 - 1)) + 1; //random day between 1 and 29
            randDay = (int) (Math.random() * (31 - 1)) + 1; //random day between 1 and 31
            String result = "M";
            if (i > 600)
                if(i%2==1)
                    result = "M";
                else
                    result = "F";
            if (insertDependent(i, "Name" + i, result,
                    new Date(randYear, randMonth, randDay), "child", conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    public static void insertSchema2(Connection connection) {
        populateEmployee(connection);
        insert_Q6(connection);
        populateDepartment(connection);
        populateDeptLocations(connection);
        populateProject(connection);
        populateWorksOn(connection);
        populateDependent(connection);
    }

    public static void main(String[] argv) {

        System.out.println("-------- PostgreSQL "
                + "JDBC Connection Testing ------------");

        try {

            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your PostgreSQL JDBC Driver? "
                    + "Include in your library path!");
            e.printStackTrace();
            return;

        }

        System.out.println("PostgreSQL JDBC Driver Registered!");

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:5432/schema2", "postgres",
                    "1234");
            insertSchema2(connection);


        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;

        }

        if (connection != null) {
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection!");
        }
    }
}
