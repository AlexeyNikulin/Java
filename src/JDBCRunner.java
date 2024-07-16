import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "Cars";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            // TODO show all tables
            getCarsModel(connection); System.out.println();
            getCarsBodies(connection); System.out.println();
            getEngine(connection); System.out.println();

            // TODO show with param
            getCarsByEngineCapacity(connection, 1.5); System.out.println(); // Получить все модели машин и их объем двигателя где их объем выше N л
            getEnginesByEngineCapacity(connection, 1.58); System.out.println(); // Получить двигатели с объемом двигателя ниже N л
            getCarsBodiesByBodyStrength(connection, 50); System.out.println(); // Получить кузовова где значение прочности больше N %

            // TODO show without duplicates
            getMaxSpeedWithoutDuplicated(connection); System.out.println(); // Получить все максимальные скорости которые есть у машин без дубликатов

            // TODO show with sorted
            getAllModelAndHorsepowerOrderByHorsepowerDesc(connection); System.out.println(); // Получить все модели и мощности машин и отсортировать по мощности в порядке убывая
            getEnginesOrderByHorsepowerDesc(connection); System.out.println(); // Получить все двигатели отсорт по мощности по убыванию

            // TODO correction
            addCarModel(connection, "K653AP91", 412568, 5342346, "NIVA", 11.00, 200.00); System.out.println(); // Добавить модель NIVA
            correctCarsModel(connection, "NIVA", 210.00); System.out.println();  // Изменить максимальную скорость у NIVA на 210
            removeCarsModel(connection, "NIVA"); System.out.println();  // Удалить модель NIVA

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных)
            // возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // Проверка окружения и доступа к базе данных

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту!");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе");
            throw new RuntimeException(e);
        }
    }

    public static void getCarsModel(Connection connection) throws SQLException {
        System.out.println("Наши тачки");
        String param = "";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM cars_model");
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();  // сколько столбцов в ответе

        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += meta.getColumnLabel(i) + ": " + rs.getString(i);
                if (i != count) param +=  " | ";
            }
            System.out.println(param);
            param = "";
        }
    }

    public static void getCarsBodies(Connection connection) throws SQLException {
        System.out.println("Кузов автомобиля");
        // имена столбцов
        String columnName0 = "id",              // id
                columnName1 = "body_strength";  // прочность
        // значения ячеек
        int param0 = -1, param1 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM cars_bodies");

        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getInt(columnName1);
            System.out.println(param0 + " | " + param1);
        }
    }

    public static void getEngine(Connection connection) throws SQLException {
        System.out.println("Движки");
        // имена столбцов
        String columnName0 = "id",              // id
                columnName1 = "horsepower",  // прочность
                columnName2 = "engine_capacity";  // прочность
        // значения ячеек
        int param0 = -1, param1 = -1;
        double param2 = -1.0;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM engine");

        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getInt(columnName1);
            param2 = rs.getDouble(columnName2);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    public static void getMaxSpeedWithoutDuplicated(Connection connection) throws SQLException {
        String param = "";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT DISTINCT max_speed AS \"Все возможные скорости\" FROM cars_model;");
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();  // сколько столбцов в ответе
        System.out.println(meta.getColumnLabel(1));
        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i) + "км/ч";
            }
            System.out.println(param);
            param = "";
        }
    }

    public static void getAllModelAndHorsepowerOrderByHorsepowerDesc(Connection connection) throws SQLException {
        String param = "";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT cars_model.model AS \"Модель\", engine.horsepower AS \"Мощность\"\n" +
                "FROM cars_model\n" +
                "JOIN engine ON cars_model.id_engine = engine.id\n" +
                "ORDER BY engine.horsepower DESC;");
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();  // сколько столбцов в ответе
        System.out.println(meta.getColumnLabel(1) + " | " + meta.getColumnLabel(2));
        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i);
                if (i != count) param += " | ";
            }
            System.out.println(param);
            param = "";
        }
    }

    public static void getEnginesOrderByHorsepowerDesc(Connection connection) throws SQLException {
        String param = "";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM engine ORDER BY engine_capacity DESC;");
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();  // сколько столбцов в ответе
        System.out.println(meta.getColumnLabel(1) + " | " + meta.getColumnLabel(2) + " | " + meta.getColumnLabel(3));
        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i);
                if (i != count) param += " | ";
            }
            System.out.println(param);
            param = "";
        }
    }

    public static void getCarsByEngineCapacity(Connection connection, double engineCapacity) throws SQLException {
        if (engineCapacity < 0 ) return;

        PreparedStatement statement = connection.prepareStatement(
                "SELECT cars_model.model AS \"Модели машин\", engine.engine_capacity AS \"Объем двигателя\" " +
                    "FROM cars_model JOIN engine ON engine.id = cars_model.id_engine " +
                    "WHERE engine.engine_capacity > ?;"
        );
        statement.setDouble(1, engineCapacity);
        ResultSet rs = statement.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            System.out.print(meta.getColumnLabel(i));
            if (i != count) System.out.print(" | ");
            else System.out.print("\n");
        }
        while(rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getDouble(2));
        }
    }

    public static void getEnginesByEngineCapacity(Connection connection, double engineCapacity) throws SQLException {
        if (engineCapacity < 0 ) return;

        PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM engine WHERE engine_capacity < ?;"
        );
        statement.setDouble(1, engineCapacity);
        ResultSet rs = statement.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            System.out.print(meta.getColumnLabel(i));
            if (i != count) System.out.print(" | ");
            else System.out.print("\n");
        }
        while(rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getDouble(2) + " | " + rs.getDouble(3));
        }
    }

    public static void getCarsBodiesByBodyStrength(Connection connection, int bodyStrength) throws SQLException {
        if (bodyStrength < 0 ) return;

        PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM cars_bodies WHERE body_strength > ?;"
        );
        statement.setInt(1, bodyStrength);
        ResultSet rs = statement.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            System.out.print(meta.getColumnLabel(i));
            if (i != count) System.out.print(" | ");
            else System.out.print("\n");
        }
        while(rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getDouble(2));
        }
    }

    public static void addCarModel(Connection connection, String car_number, int id_car_bodies, int id_engine, String model, Double fuel_consumption, Double max_speed)
            throws SQLException {

        if (id_car_bodies < 0 || id_engine < 0 || model.isBlank() || fuel_consumption <= 0  || max_speed <= 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO cars_model(car_number, id_cars_bodies, id_engine, model, fuel_consumption, max_speed) " +
                        "VALUES (?, ?, ?, ?, ?, ?) returning car_number;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, car_number);
        statement.setInt(2, id_car_bodies);
        statement.setInt(3, id_engine);
        statement.setString(4, model);
        statement.setDouble(5, fuel_consumption);
        statement.setDouble(6, max_speed);

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор машины " + rs.getString(1));
        }

        System.out.println("INSERTed " + count + " cars");
        getCarsModel(connection);
    }

    private static void correctCarsModel(Connection connection, String model, Double max_speed)
            throws SQLException {
        if (model.isBlank() || max_speed <= 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE cars_model SET max_speed=? WHERE model=?;");
        statement.setString(2, model);
        statement.setDouble(1, max_speed);

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " cars");
        getCarsModel(connection);
    }

    private static void removeCarsModel(Connection connection, String model)
            throws SQLException {
        if (model.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE FROM cars_model WHERE model=?;");
        statement.setString(1, model);

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("DELETEd " + count + " cars");
        getCarsModel(connection);
    }
}

