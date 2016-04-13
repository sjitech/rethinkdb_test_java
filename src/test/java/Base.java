import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Database;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;

import static com.rethinkdb.RethinkDB.r;

public class Base {
    static final Logger logger = LoggerFactory.getLogger(Base.class);
    Database db = new Database(System.getProperty("dbName", /*default db name:*/"test"));

    static class C {
        static final int K = 1000;
        static final int M = 10000;

        static final long userCount = Long.valueOf(System.getProperty("userCount",
                String.valueOf(100 * M)));

        static final long conducteurCount = (long) (userCount * Float.valueOf(System.getProperty("conducteurRatio",
                String.valueOf(0.3))));

        static final long passengerCount = (long) (userCount * Float.valueOf(System.getProperty("passengerRatio",
                String.valueOf(0.8))));

        static final long tourCount = Long.valueOf(System.getProperty("tourCount",
                String.valueOf(100 * M)));

        static final long bookingCount = Long.valueOf(System.getProperty("bookingCount",
                String.valueOf(200 * M)));

        static final int passengersPerTour = Integer.valueOf(System.getProperty("passengersPerTour",
                String.valueOf(3)));

        static final int bookingsPerTour = Math.max((int) divUp(bookingCount, tourCount), passengersPerTour + 1);
        static final long bookingTourCount = divUp(bookingCount, bookingsPerTour);

        static final long approvedBookingTourCount = bookingTourCount -
                (bookingCount % bookingsPerTour == 0 || bookingCount % bookingsPerTour >= passengersPerTour ? 0 : 1);
        static final long approvedBookingCount = approvedBookingTourCount * passengersPerTour;

        static final long reviewCount = approvedBookingCount * 2;

        static long divUp(long a, long b) {
            return (a + b - 1) / b;
        }
    }

    static class Format {
        static final String userId = "user%0" + String.valueOf(C.userCount).length() + "d";
        static final String tourId = "tour%0" + String.valueOf(C.tourCount).length() + "d";
        static final String bookingId = "booking%0" + String.valueOf(C.bookingCount).length() + "d";
        static final String reviewId = "review%0" + String.valueOf(C.reviewCount).length() + "d";
    }

    static class S {
        static final String index = "index";
        static final String left = "left";
        static final String right = "right";
        static final String array = "array";
        static final String reduction = "reduction";
        static final String group = "group";
        static final String replaced = "replaced";
        static final String array_limit = "array_limit";

        static final String users = "users";
        static final String userId = "userId";
        static final String internalId = "internalId";

        static final String tours = "tours";
        static final String tourId = "tourId";
        static final String tourIdAndStatus = "tourIdAndStatus";

        static final String bookings = "bookings";
        static final String bookingId = "bookingId";

        static final String reviews = "reviews";
        static final String reviewId = "reviewId";
        static final String byUserId = "byUserId";
        static final String ofUserId = "ofUserId";
        static final String starRank = "starRank";
        static final String tourIdAndbyUserIdAndofUserId = "tourIdAndbyUserIdAndofUserId";

        static final String createdAt = "createdAt";
        static final String comment = "comment";
        static final String status = "status";

        static final String approved = "approved";
        static final String canceled = "canceled";

        static final String _passengerIdAry = "_passengerIdAry";
        static final String _conducteurId = "_conducteurId";

        static final String _selfReviews = "_selfReviews";
        static final String _selfBookings = "_selfBookings";
    }
}
