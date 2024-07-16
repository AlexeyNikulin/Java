import java.sql.*;

public class Main {
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Connected JDBC Driver");

            Connection connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost/Cars",
                    "postgres",
                    "postgres");

            System.out.println("Connected database cars");

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM cars_model;");
            ResultSetMetaData meta = resultSet.getMetaData();
//            System.out.println(meta.getColumnCount());

//            while (resultSet.next()){  // пока есть данные
////                System.out.println(resultSet.getString(1));
//                for (int i = 1; i <= meta.getColumnCount(); i++) {
//                    String l = meta.getColumnLabel(i); //название столбца
//                    int t = meta.getColumnType(i); // код типа данных БД
//                    String tn = meta.getColumnTypeName(i); // название типа данных БД
//                    String cn = meta.getColumnClassName(i); // соответствующий тип данных
//                    int s = meta.getColumnDisplaySize(i);
//                    System.out.println(i);
//                    System.out.printf("Название столбца: %s%n", l);
//                    System.out.printf("код типа данных БД: %d%n", t);
//                    System.out.printf("название типа данных БД: %s%n", tn);
//                    System.out.printf("соответствующий тип данных: %s%n", cn);
//                    System.out.printf("Название столбца: %d%n", s);
//                    System.out.printf("Само значение в ячейке: %s%n", resultSet.getString(i));
//                    System.out.println("\n");
//                }
//            }

//            INSERT INTO cars_model VALUES(DEFAULT, 452134, 5342346, 'ВАЗ-2104', 7.8, 180);

            int rows = statement.executeUpdate(
                    "INSERT INTO cars_model VALUES(DEFAULT, 452134, 5342346, 'ВАЗ-2106', 7.8, 180) returning model, id;",
                    Statement.RETURN_GENERATED_KEYS);
            ResultSet resultSet1 = statement.getGeneratedKeys();
            System.out.println(resultSet1.next());
            System.out.println(resultSet1.getString(1));
            System.out.println(resultSet1.getString(2));


            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT * FROM cars_model WHERE model = ?;");

            preparedStatement.setString(1, "ВАЗ-2107");

            statement.close();
            connection.close();

        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC Driver is not found. Include it in your library path");
        } catch (SQLException e) {
            System.out.println(e.getErrorCode());
            System.out.println(e.getMessage());
            throw new RuntimeException("Не удалось подключиться к базе данных");
        }
    }
}