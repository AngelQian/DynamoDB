/**
 * Created by AngelQian on 3/2/16.
 */
public class DDB {
    public static void main(String[] args){
        System.out.print("test ok");
        db d = new db();
//        d.createTB();
        d.insertData();
        d.updateData("set append.info = :s", ":s", "be sad");
        d.deleteData("append.info = :s", ":s", "be sad");
        d.queryAllData();
        d.queryData("#nm = :d", ":d", "abc");
        d.listAllTBs();
//        d.deleteTB();
    }
}
