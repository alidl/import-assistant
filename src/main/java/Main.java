import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Date;
import java.sql.*;
import java.text.CollationKey;
import java.text.Collator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    static class Person {
        // person
        String first_name = "", last_name = "";
        String gender = "", birthdate = "";
        ArrayList<Integer> starbase_ids = new ArrayList<>();

        ArrayList<String> accreditations = new ArrayList<>();
        ArrayList<String> invitations = new ArrayList<>();

        // address
        Timestamp latest_address = new Timestamp(new GregorianCalendar(1990, Calendar.JANUARY, 1).getTime().getTime());
        String company = "";
        String department_and_position = "";
        String street = "";
        String city = "";
        String zip = "";
        String country = "";
        TreeSet<String> emails = new TreeSet<>();
        TreeSet<String> phones = new TreeSet<>();

        // group_link
        ArrayList<String> categories = new ArrayList<>();

        // interests i professions
        ArrayList<String> interest_profession = new ArrayList<>();


    }

    public static void main(String... args) {

        try {
            System.out.println("Connecting...");
            Connection con;
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            String url = "jdbc:sqlserver://starbase.example.com:7559;databaseName=Example";
            con = DriverManager.getConnection(url, "username", "password");

            System.out.println("Preparing queries...");
            PreparedStatement ps_persons = con.prepareStatement(
                    "select id_person, first_name, last_name, ID_GENDER, BIRTH_DATE from persons " +
                            "where PERSON_TYPE = 1");

            PreparedStatement ps_address = con.prepareStatement(
                    "select id_person, phone_1, phone_2, mobile, email_1, email_2, " +
                            "company, department, position, street, zip, city, COUNTRY, MODIFY_DATE from address a " +
                            "join COUNTRIES c on a.ID_COUNTRY = c.ID_COUNTRY");

            PreparedStatement ps_intersts = con.prepareStatement(
                    "select id_person, i.INTEREST_local from INTEREST_LINK il " +
                            "join INTERESTS i on il.ID_INTEREST = i.ID_INTEREST");
            PreparedStatement ps_professions = con.prepareStatement(
                    "select id_person, p.PROFESSION from PROFESSION_LINK pl " +
                            "join PROFESSIONS p on pl.ID_PROFESSION = p.ID_PROFESSION");

            PreparedStatement ps_group = con.prepareStatement(
                    "select id_person, GROUP_NAME, section_name from group_link gl " +
                            "join groups g on gl.ID_GROUP = g.ID_GROUP " +
                            "join sections s on gl.ID_SECTION = s.ID_SECTION");

            PreparedStatement ps_invitations = con.prepareStatement(
                    "select id_person, INVITATION_TYPE, cast(EDITION_YEAR as varchar(4)) as edition_year, r.REPLY from invitations i " +
                            "join INVITATION_TYPES it on i.ID_INVITATION_TYPE = it.ID_INVITATION_TYPE " +
                            "join EDITIONS e on i.ID_EDITION = e.ID_EDITION " +
                            "join REPLY r on i.ID_REPLY = r.ID_REPLY");
            PreparedStatement ps_accreditations = con.prepareStatement(
                    "select id_person, act.ACCREDITATION_TYPE, edition_year from ACCREDITATIONS a " +
                            "join ACCREDITATION_TYPES act on act.ID_ACCREDITATION_TYPE = a.ID_ACCREDITATION_TYPE " +
                            "join editions e on a.ID_EDITION = e.ID_EDITION");


            HashMap<Integer, Person> id_map = new HashMap<>();
            HashMap<CollationKey, Person> name_map = new HashMap<>();

            System.out.println("Importing names...");
            {
                Collator collator = Collator.getInstance();
                collator.setStrength(Collator.PRIMARY);


                ResultSet rs = ps_persons.executeQuery();
                while (rs.next()) {
                    String first_name = nonull_trim(rs.getNString("first_name"));
                    String last_name = nonull_trim(rs.getNString("last_name"));
                    String canonical_name = first_name + " " + last_name;
                    if (canonical_name.trim().isEmpty()) continue;

                    CollationKey collationKey = collator.getCollationKey(canonical_name);

                    int starbase_id = rs.getInt("id_person");
                    Person p;
                    if (name_map.containsKey(collationKey)) {
                        p = name_map.get(collationKey);
                    } else {
                        p = new Person();
                        name_map.put(collationKey, p);
                    }
                    id_map.put(starbase_id, p);
                    p.starbase_ids.add(starbase_id);

                    if (!p.last_name.endsWith("ć"))
                        p.last_name = last_name;

                    p.first_name = first_name;
                    // gender 2: male 3: female
                    int gender_int = rs.getInt("id_gender");
                    if (gender_int == 2) p.gender = "M";
                    if (gender_int == 3) p.gender = "F";
                    Date birthdate = rs.getDate("birth_date");
                    if (birthdate != null) p.birthdate = dateFormat.format(birthdate);
                }
            }

            System.out.println("Importing addresses...");
            {
                ResultSet rs = ps_address.executeQuery();

                while (rs.next()) {
                    int starbase_id = rs.getInt("id_person");
                    Person p = id_map.get(starbase_id);
                    if (p == null) continue;

                    Timestamp vrijeme = rs.getTimestamp("modify_date");
                    normalize_string_and_add(rs.getString("phone_1"), p.phones);
                    normalize_string_and_add(rs.getString("phone_2"), p.phones);
                    normalize_string_and_add(rs.getString("mobile"), p.phones);
                    normalize_string_and_add(rs.getString("email_1"), p.emails);
                    normalize_string_and_add(rs.getString("email_1"), p.emails);

                    boolean noviji_podaci = p.latest_address.before(vrijeme);
                    if (noviji_podaci)
                        p.latest_address = vrijeme;

                    String department = nonull_trim(rs.getNString("department"));
                    String position = nonull_trim(rs.getNString("position"));

                    String pozdep = department;
                    if (department.isEmpty())
                        pozdep = position; // department je prazan
                    else
                        if (!position.isEmpty()) // oba su tu
                            pozdep = department + " / " + position;

                    p.department_and_position = pick_string(p.department_and_position, pozdep, noviji_podaci);
                    p.company = pick_string(p.company, rs.getNString("company"), noviji_podaci);
                    p.street = pick_string(p.street, rs.getNString("street"), noviji_podaci);
                    p.city = pick_string(p.city, rs.getNString("city"), noviji_podaci);
                    p.zip = pick_string(p.zip, rs.getNString("zip"), noviji_podaci);
                    p.country = pick_string(p.country, rs.getNString("country"), noviji_podaci);
                }
            }

            System.out.println("Importing classification...");
            {
                ResultSet rs = ps_group.executeQuery();

                while (rs.next()) {
                    int starbase_id = rs.getInt("id_person");
                    Person p = id_map.get(starbase_id);
                    if (p == null) continue;
                    p.categories.add(rs.getString("group_name") +
                            "/" + rs.getString("section_name"));
                }
            }

            System.out.println("Importing interests...");
            {
                ResultSet rs = ps_intersts.executeQuery();

                while (rs.next()) {
                    int starbase_id = rs.getInt("id_person");
                    Person p = id_map.get(starbase_id);
                    if (p == null) continue;
                    //p.interest_profession.add("Interests/" + rs.getNString("interest_local"));
                    p.categories.add(rs.getNString("interest_local"));
                }
            }

            System.out.println("Importing professions...");
            {
                ResultSet rs = ps_professions.executeQuery();

                while (rs.next()) {
                    int starbase_id = rs.getInt("id_person");
                    Person p = id_map.get(starbase_id);
                    if (p == null) continue;
                    p.interest_profession.add("Proffesions/Import/" + rs.getNString("profession"));
                }
            }


            System.out.println("Importing invitations...");
            {
                ResultSet rs = ps_invitations.executeQuery();

                while (rs.next()) {
                    int starbase_id = rs.getInt("id_person");
                    Person p = id_map.get(starbase_id);
                    if (p == null) continue;
                    p.invitations.add("P" + rs.getString("edition_year") + ": " +
                            rs.getNString("invitation_type") + " (" + rs.getNString("reply") + ")");
                }
            }

            System.out.println("Importing accreditations...");
            {
                ResultSet rs = ps_accreditations.executeQuery();

                while (rs.next()) {
                    int starbase_id = rs.getInt("id_person");
                    Person p = id_map.get(starbase_id);
                    if (p == null) continue;
                    p.invitations.add("A" + rs.getString("edition_year") + ": " + rs.getNString("accreditation_type"));
                }
            }

            System.out.println("Generating spreadsheet...");
            {
                Workbook workbook = new HSSFWorkbook(new FileInputStream("template.xls"));
                Sheet sheet = workbook.getSheet("List1");

                int row_number = 1;

                for (Map.Entry<CollationKey, Person> pair : name_map.entrySet()) {
                    Person p = pair.getValue();
                    Row row = sheet.createRow(row_number++);
                    row.createCell(0).setCellValue(p.company);
                    row.createCell(2).setCellValue(p.first_name);
                    row.createCell(3).setCellValue(p.last_name);
                    row.createCell(5).setCellValue(p.department_and_position);
                    row.createCell(6).setCellValue(p.street);
                    row.createCell(7).setCellValue(p.city);
                    row.createCell(8).setCellValue(p.zip);
                    row.createCell(9).setCellValue(p.country);
                    if (p.phones.size() >= 1)
                        row.createCell(15).setCellValue(p.phones.first());
                    if (p.phones.size() >= 2)
                        row.createCell(18).setCellValue(p.phones.last());
                    row.createCell(22).setCellValue(String.join("|;|", p.emails));
                    row.createCell(26).setCellValue(String.join("|;|", p.interest_profession));
                    row.createCell(28).setCellValue(String.join("|;|", p.categories));
                    String note = "Imported from StarBase ID: " +
                            p.starbase_ids.stream().map(Object::toString).collect(Collectors.joining(",")) + "\n";
                    note += String.join("\n", p.accreditations);
                    note += String.join("\n", p.invitations);
                    row.createCell(32).setCellValue(note);
                    row.createCell(40).setCellValue(p.birthdate);
                    row.createCell(41).setCellValue(p.gender);
                    row.createCell(44).setCellValue(p.country);
                    row.createCell(45).setCellValue(p.country);
                }


                FileOutputStream fileOut = new FileOutputStream("generated-file-for-import.xls");
                workbook.write(fileOut);
                fileOut.close();

                workbook.close();
                System.out.println("Done!");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static String nonull_trim(String str) {
        if (str == null) return "";
        else return str.trim();
    }

    static void normalize_string_and_add(String source, Set<String> collection) {
        if (source == null)
            return;

        String trimmed = source.trim();
        if (!trimmed.isEmpty())
            collection.add(trimmed);
    }

    static String pick_string(String first, String second, boolean first_is_older_than_second) {
        if (second == null) return first;
        if (second.equals("none") || second.equals("INACTIVE")) return first;
        if (first.isEmpty()) return second.trim();

        if (first_is_older_than_second) return second.trim();
        else return first;
    }
}