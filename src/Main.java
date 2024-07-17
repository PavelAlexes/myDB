import java.sql.*;

public class Main {

    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";

    private static final String DATABASE_NAME = "hockey_team";
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;

        public static void main(String[] args) {

            // проверка возможности подключения
            checkDriver();
            checkDB();
            System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

            // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
            try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
//                getTeams(connection); System.out.println();
//                  getCoachWins(connection, "lokom"); System.out.println();
//                getAllPlayers(connection); System.out.println();
//                addPlayer(connection, "pasha", "pol", "нападающий", "loko",0,88); System.out.println();
//                removePlayer(connection, "pasha", "pol", "нападающий", "loko",0,88); System.out.println();
//                correctNumberOfPoints(connection, "pasha", "pol", "нападающий", "loko",4,88); System.out.println();
//                getPlayerByNamed(connection, "pasha", false); System.out.println();// возьмем всех и найдем перебором
//                getCoachPlayer(connection, "loko"); System.out.println();







                getPlayerByNamed(connection, "pasha", true); System.out.println(); // тоже самое сделает БД   ???????????????????????
//                getContracts(connection); System.out.println();
//


            } catch (SQLException e) {
                // При открытии соединения, выполнении запросов могут возникать различные ошибки
                // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
                if (e.getSQLState().startsWith("23")){
                    System.out.println("Произошло дублирование данных");
                } else throw new RuntimeException(e);
            }
//        finally { // или так закрываем соединение с базой данных, но close() также выбрасывает SQLException
//            if (statement != null) statement.close();
//            if (connection != null) connection.close();
//        }
        }


    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }


    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }









    static void getContracts (Connection connection) throws SQLException {
        String param = "";

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM coach;");   // выполняем запроса на поиск и получаем список ответов

        int count = rs.getMetaData().getColumnCount();  // сколько столбцов в ответе
        for (int i = 1; i <= count; i++){
            // что в этом столбце?
            System.out.println("position - " + i +
                    ", label - " + rs.getMetaData().getColumnLabel(i) +
                    ", type - " + rs.getMetaData().getColumnType(i) +
                    ", typeName - " + rs.getMetaData().getColumnTypeName(i) +
                    ", javaClass - " + rs.getMetaData().getColumnClassName(i)
            );
        }
        System.out.println();
        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i);
                if (i != count) param += " | ";
            }
            System.out.println(param);
            param = "";
        }
    }




    private static void getCoachWins(Connection connection, String team) throws SQLException{
        if (team == null || team.isBlank()) return;// проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT coach.first_name, coach.last_name, number_of_wins " +
                        "FROM coach " +
                        "JOIN team ON coach.team = team.title " +
                        "WHERE coach.team = ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, team);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " " + rs.getString(2) + " | " + rs.getString(3) + " wins!");
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");


    }


    private static void getCoachPlayer(Connection connection, String team) throws SQLException {
        if (team == null || team.isBlank()) return;// проверка "на дурака"
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT coach.first_name, player.first_name " +
                        "FROM team " +
                        "JOIN coach ON coach.team = team.title " +
                        "JOIN player ON player.team = team.title " +
                        "WHERE team.title = ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, team);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }


private static void getTeams(Connection connection) throws SQLException{
    // имена столбцов
    String columnName0 = "id", columnName1 = "title", columnName2 ="number_of_wins";
    // значения ячеек
    int param0 = -1;
    String param1 = null, param2 = null;

    Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
    ResultSet rs = statement.executeQuery("SELECT * FROM team;"); // выполняем запроса на поиск и получаем список ответов

    while (rs.next()) {  // пока есть данные, продвигаться по ним
        param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
        param1 = rs.getString(columnName1);
        param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
        System.out.println(param0 + " | " + param1 + " | " + param2);
    }
}

    static void getAllPlayers (Connection connection) throws SQLException {
        // значения ячеек
        int param0 = -1, param5 = -1, param6 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM player;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getString(4);
            param4 = rs.getString(5);
            param5 = rs.getInt(6);
            param6 = rs.getInt(7);

            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4
                    + " | " + param5 + " | " + param6);
        }
    }



    private static void getPlayerByNamed(Connection connection, String first_name, boolean fromSQL) throws SQLException {
        if (first_name == null || first_name.isBlank()) return;// проверка "на дурака"

        if (fromSQL) {
            getPlayerNamed(connection, first_name);               // если флаг верен, то выполняем аналогичный запрос c условием (WHERE)
        } else {
            Statement statement = connection.createStatement();      // создаем оператор для простого запроса (без параметров)
            ResultSet rs = statement.executeQuery(
                    "SELECT * " +
                            "FROM player;");
            while (rs.next()) {  // пока есть данные перебираем их
                if (rs.getString(2).contains(first_name)) { // и выводим только определенный параметр
                    System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3) +
                            " | " + rs.getString(4) + " | " + rs.getString(5) + " | " + rs.getInt(6) + " | " + rs.getInt(7) );
                }
            }
        }
    }

    private static void getPlayerNamed(Connection connection, String first_name) throws SQLException {
        if (first_name == null || first_name.isBlank()) return; // проверка "на дурака"
        first_name = '%' + first_name + '%'; // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT * " +
                        "FROM player;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, first_name);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) +
                    " | " + rs.getString(4) + " | " + rs.getInt(5) + " | " + rs.getInt(6));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void addPlayer (Connection connection, String first_name, String last_name, String role,
                                   String team, int number_of_points, int game_number)  throws SQLException {
        if (first_name == null || first_name.isBlank() || last_name == null || last_name.isBlank() ||
                role == null || role.isBlank() || team == null || team.isBlank() || number_of_points < 0 || game_number < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO player(first_name, last_name, role, team, number_of_points, game_number) VALUES (?, ?, ?, ?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, first_name);    // "безопасное" добавление имени
        statement.setString(2, last_name);
        statement.setString(3, role);
        statement.setString(4, team);
        statement.setInt(5, number_of_points);
        statement.setInt(6, game_number);// "безопасное" добавление количества гла

        int count =
                statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("ID player " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " player");
        getAllPlayers(connection);
    }

    private static void correctNumberOfPoints (Connection connection, String first_name, String last_name, String role,
                                       String team, int number_of_points, int game_number) throws SQLException {
        if (first_name == null || first_name.isBlank() || last_name == null || last_name.isBlank() ||
                role == null || role.isBlank() || team == null || team.isBlank() || number_of_points < 0 || game_number < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE player SET number_of_points = ? WHERE game_number = ?;");
        statement.setInt(1, number_of_points); // сначала что передаем
        statement.setInt(2, game_number);   // затем по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " player");
        getAllPlayers(connection);
    }

    private static void removePlayer(Connection connection, String first_name, String last_name, String role,
                                     String team, int number_of_points, int game_number) throws SQLException {
        if (first_name == null || first_name.isBlank() || last_name == null || last_name.isBlank() ||
                role == null || role.isBlank() || team == null || team.isBlank() || number_of_points < 0 || game_number < 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from player WHERE game_number = ?;");
        statement.setInt(1, game_number);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " player");
        getAllPlayers(connection);
    }
}
