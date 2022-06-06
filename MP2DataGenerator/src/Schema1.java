import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;

public class Schema1 {
	public static int id_course = 0;
	public static int id_instructor = 0;
	public static int id_student = 0;
	public static int id_dep = 0;
	public static String y = "600";
	public static int count_loop = 0;

	// //////////////////////////////////////////// Table Insertion Methods
	// ///////////////////////////////////////////////////////////////
	public static long insertDepartment(int building, String deptName, int budget, Connection conn) {
		String SQL = "INSERT INTO department(dep_name,building,budget) " + "VALUES(?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, building);
			pstmt.setString(1, deptName);
			pstmt.setInt(3, budget);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertInstructor(int ID, String name, int salary, String deptName, Connection conn) {
		String SQL = "INSERT INTO instructor(ID,name,salary,dep_name)" + "VALUES(?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, name);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, salary);
			pstmt.setString(4, deptName);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertClassroom(int building, int roomNo, int capacity, Connection conn) {
		String SQL = "INSERT INTO classroom(building,room_number,capacity)" + "VALUES(?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, roomNo);
			pstmt.setInt(1, building);
			pstmt.setInt(3, capacity);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertTimeSlot(int ID, String day, Time start, Time end, Connection conn) {
		String SQL = "INSERT INTO time_slot(id,day,start,end_time)" + "VALUES(?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, day);
			pstmt.setInt(1, ID);
			pstmt.setTime(3, start);
			pstmt.setTime(4, end);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	public static long insertStudent(int ID, String name, int credit, String deptName, int instID, Connection conn) {
		String SQL = "INSERT INTO student(id,name,tot_credit,department,advisor_id)" + "VALUES(?,?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, name);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, credit);
			pstmt.setString(4, deptName);
			pstmt.setInt(5, instID);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE course(course_id INT PRIMARY KEY, title VARCHAR(20), credits
	// INT, department VARCHAR(20) REFERENCES department);
	public static long insertCourse(int ID, String title, int credit, String deptName, Connection conn) {
		String SQL = "INSERT INTO course(course_id,title,credits,department)" + "VALUES(?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(2, title);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, credit);
			pstmt.setString(4, deptName);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE pre_requiste(course_id INT, prereq_id INT,PRIMARY
	// KEY(course_id, prereq_id));
	public static long insertPrerequiste(int ID, int preID, Connection conn) {
		String SQL = "INSERT INTO pre_requiste(course_id,prereq_id)" + "VALUES(?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, preID);
			pstmt.setInt(1, ID);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE section(section_id INT PRIMARY KEY, semester INT, year INT,
	// instructor_id INT REFERENCES instructor, course_id INT REFERENCES
	// course,classroom_building INT REFERENCES classroom(building),
	// classroom_room_no INT REFERENCES classroom(room_number));

	public static long insertSection(int ID, int semester, int year, int instID, int courseID, int classroomBuilding,
									 int classroomRoomNo, Connection conn) {
		String SQL = "INSERT INTO section(section_id,semester,year,instructor_id,course_id,classroom_building,classroom_room_no)"
				+ "VALUES(?,?,?,?,?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, semester);
			pstmt.setInt(1, ID);
			pstmt.setInt(3, year);
			pstmt.setInt(4, instID);
			pstmt.setInt(5, courseID);
			pstmt.setInt(6, classroomBuilding);
			pstmt.setInt(7, classroomRoomNo);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE takes(student_id INT REFERENCES student, section_id INT
	// REFERENCES section, grade real, PRIMARY KEY(student_id, section_id));
	public static long insertTakes(int ID, int secID, double grade, Connection conn) {
		String SQL = "INSERT INTO takes(student_id,section_id,grade)" + "VALUES(?,?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, secID);
			pstmt.setInt(1, ID);
			pstmt.setDouble(3, grade);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// CREATE TABLE section_time(time_slot INT REFERENCES time_slot, section_id
	// INT REFERENCES section, PRIMARY KEY(time_slot, section_id));
	public static long insertSectionTime(int timeSlot, int secID, Connection conn) {
		String SQL = "INSERT INTO section_time(time_slot,section_id)" + "VALUES(?,?);";

		long id = 0;
		try {
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS);

			pstmt.setInt(2, secID);
			pstmt.setInt(1, timeSlot);

			int affectedRows = pstmt.executeUpdate();
			System.out.println("Number of affected rows is " + affectedRows);
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					// System.out.println(rs.next());
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

	// ///////////////////////////////////////// Data Population Method
	// //////////////////////////////////////////////////////
	public static void populateDepartment(Connection conn, String Name, int Build) {
		for (int i = 0; i < 1; i++) {

			if (insertDepartment(2, Name, Build, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateDepartmentnew(Connection conn) {
		for (int i = 1; i < 2; i++) {

			if (insertDepartment(i, "CSEN", i, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateInstructor(Connection conn) {
		for (int i = 1; i < 35; i++) {
			int a = 1000;

			if (insertInstructor(i, "Name" + i, i * a, "CSEN", conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateClassroom(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertClassroom(i, i, 100 + i, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	@SuppressWarnings("deprecation")
	public static void populateTimeSlot(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertTimeSlot(i, "day" + i, new Time(12, 0, 0), new Time(13, 0, 0), conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateStudent(Connection conn) {
		for (int i = 1; i < 1200; i++) {
			int ran = ((int) (Math.random() * (34 - 1))) + 1;
			int ran2 = ((int) (Math.random() * (50 - 20))) + 20;
			if (insertStudent(i, "Student" + i, ran2, "CSEN", ran, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateCourse(Connection conn, int Coursecount, String Depname, String Coursename) {
		for (int i = 45; i < Coursecount + 45; i++) {
			String y = "600";
			int z = Integer.parseInt(y) + i;
			if (insertCourse(i, Coursename + z, i, Depname, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populatePrerequiste(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertPrerequiste(i, i, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void populateSection(Connection conn) {
		int j = 1;
		for (int i = 1; i < 2; i++){
			//int randSemester = (in)Math.random()*(10-1)+1;
			if (insertSection(i, i, 2019, i, i, j, j, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
			j++;
		}
	}

	public static void Insert(Connection conn,int count_dep, int count_course, int count_instructor, int count_student, String Coursename) {
		id_dep++;
		if (id_dep == 1) {
			insertDepartment(53, "CSEN",30000, conn);

			String y = "600";
			for (int i = 0; i < count_course; i++) {
				int ran_credit = ((int) (Math.random() * (50 - 20))) + 20;
				int z = Integer.parseInt(y) + i;
				id_course++;
				if (insertCourse(id_course, "CSEN" + z, ran_credit, "CSEN", conn) == 0) {
					System.err.println("insertion of record " + i + " failed");
					break;
				} else
					System.out.println("insertion was successful");
			}


			for (int i = 0; i < count_instructor; i++) {
				id_instructor++;
				count_loop++;
				int ransalary = ((int) (Math.random() * (60000 - 20000))) + 20000;
				if (insertInstructor(id_instructor, "Instructor" + id_instructor, ransalary, "CSEN", conn) == 0) {
					System.err.println("insertion of record " + i + " failed");
					break;
				} else
					System.out.println("insertion was successful");
			}


			for (int i = 0; i < count_student; i++) {
				id_student++;
				int ran_instructor = ((int) (Math.random() * (id_instructor - 1))) + 1;
				int ran_credit = ((int) (Math.random() * (50 - 20))) + 20;
				if (insertStudent(id_student, "Student" + i, ran_credit, "CSEN", ran_instructor, conn) == 0) {
					System.err.println("insertion of record " + i + " failed");
					break;
				} else
					System.out.println("insertion was successful");
			}
			id_dep++;
		} if(id_dep==2) {
			for (int id_dep = 2; id_dep <= count_dep; id_dep++) {
				int ranbuild = ((int) (Math.random() * (65 - 1))) + 1;
				String x= "Dep"+Integer.toString(id_dep);
				int ranbudget = ((int) (Math.random() * (65000 - 21000))) + 21000;
				if (insertDepartment(ranbuild, x, ranbudget,conn) == 0) {
					System.err.println("insertion of record " + id_dep + " failed");
					break;
				} else {
					for (int i = 0; i < count_course; i++) {
						int ran_credit = ((int) (Math.random() * (50 - 20))) + 20;
						int z = Integer.parseInt(y) + i;
						id_course++;
						if (insertCourse(id_course, Coursename + z, ran_credit, "Dep" + id_dep, conn) == 0) {
							System.err.println("insertion of record " + i + " failed");
							break;

						}
					}
					for (int j = 0; j < count_instructor; j++) {
						id_instructor++;
						int ransalary = ((int) (Math.random() * (60000 - 20000))) + 20000;
						count_loop++;
						if (insertInstructor(id_instructor, "Instructor" + id_instructor, ransalary, "Dep" + id_dep,
								conn) == 0) {
							System.err.println("insertion of record " + j + " failed");
							break;
						}
					}
					for (int i = 0; i < count_student; i++) {
						id_student++;
						int Lower= (count_loop-count_instructor) +1;
						int ran_instructor = ((int) (Math.random() * (count_loop - Lower ))) + Lower;

						int ran_credit = ((int) (Math.random() * (50 - 20))) + 20;
						//count_loop--;
						if (insertStudent(id_student, "Student" + i, ran_credit, "Dep" + id_dep, ran_instructor,
								conn) == 0) {
							System.err.println("insertion of record " + i + " failed");
							break;

						}

					}


				}
			}
			System.out.println("Insertion of "+id_dep+"is succesfull");
		}

	}

	public static void populateTakes(Connection conn) {
		double j = 0.7;
		for (int i = 1; i < 10000; i++) {
			if (j == 5)
				j = 0.7;
			if (insertTakes(i, i, j, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
			j += 0.3;
		}
	}

	public static void populateSectionTime(Connection conn) {
		for (int i = 1; i < 10000; i++) {
			if (insertSectionTime(i, i, conn) == 0) {
				System.err.println("insertion of record " + i + " failed");
				break;
			} else
				System.out.println("insertion was successful");
		}
	}

	public static void insertSchema1(Connection connection) {
//		populateDepartment(connection);
		// populateInstructor(connection);
//		populateClassroom(connection);
//		populateTimeSlot(connection);
		// populateStudent(connection);
		// populateCourse(connection,40,"DMET","DMET");
//		populatePrerequiste(connection);
//		populateSection(connection);
//		populateTakes(connection);
//		populateSectionTime(connection);
//	populateDepartment(connection, "DMET", 4);
//	insertDepartment(1,"CSEN",30,connection);
		// Insert(connection, 30, "Denx", "DSNx");
		// Insert(connection, 30, "Den", "DSN");
		// System.out.println(iz);
		// insertDepartment(1, "CSEN", 53, connection);
		//	Insert(connection,65,30,25,1150,"Dep");
		Insert(connection,20,5,5,5,"Dep");
		//System.out.println(count_loop);
	}

	public static void main(String[] args) {

		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			e.printStackTrace();
			return;

		}

		System.out.println("PostgreSQL JDBC Driver Registered!");

		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/schema1", "postgres", "1234");
			insertSchema1(connection);

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
