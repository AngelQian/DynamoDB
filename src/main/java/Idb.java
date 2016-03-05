/**
 * Created by AngelQian on 3/2/16.
 */
public interface Idb {
    void createTB();

    void deleteTB();

    void aboutTB();

    void listAllTBs();

    //CRUD
    void insertData();

    void updateData(String updateExpression, String updateKey, String updateValue);

    void deleteData(String deleteExp, String deleteKey, String deleteVal);

    void queryAllData();

    void queryData(String queryExp, String queryKey, String queryVal);
    //db status

}
