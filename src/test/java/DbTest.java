import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import org.junit.Test;

import static com.rethinkdb.RethinkDB.r;
import static org.junit.Assert.assertEquals;

public class DbTest {
    @org.junit.Test
    public void test_array_limit() {
        Connection c = null;
        try {
            c = Connection.build().db("test").connect();
        } catch(Exception e){}

        try {
            r.tableCreate("bookings").optArg("primary_key", "bookingId").run(c);
        } catch(Exception e){}

        try {
            r.table("bookings").indexCreate("tourId").run(c);
        } catch(Exception e){}

        try {
            r.table("bookings").delete().run(c);
        } catch(Exception e){}

        r.table("bookings").insert(r.hashMap()
                .with("bookingId", "booking1")
                .with("status", "approved")
                .with("tourId", "tour1")
                .with("userId", "user1")
        ).run(c);

        r.table("bookings").insert(r.hashMap()
                .with("bookingId", "booking2")
                .with("status", "approved")
                .with("tourId", "tour1")
                .with("userId", "user2")
        ).run(c);

        long resultTourCount = r.table("bookings").group().optArg("index", "tourId")
                .g("userId").distinct().count()
                .ungroup()
                .filter(row -> row.g("reduction").eq(2))
                .count()
                .run(c, OptArgs.of("array_limit", 1));
        assertEquals(1L, resultTourCount);
    }
}
