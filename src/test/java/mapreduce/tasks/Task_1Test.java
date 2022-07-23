package mapreduce.tasks;

import junit.framework.TestCase;

public class Task_1Test extends TestCase {

    public void testIsDateInRange (){
        Task_1.Task_1_Map Task_1 = new Task_1.Task_1_Map();
        String filterDateStart = "2022-01-01";
        String filterDateEnd = "2022-01-31";
        assertTrue(Task_1.isDateInRange("2022-01-15T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertTrue(Task_1.isDateInRange("2022-01-01T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertTrue(Task_1.isDateInRange("2022-01-31T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertFalse(Task_1.isDateInRange("2022-02-02T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertFalse(Task_1.isDateInRange("2021-12-31T00:00:00+00:00", filterDateStart, filterDateEnd));

    }

}