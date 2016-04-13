import org.junit.Test;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;

import static com.rethinkdb.RethinkDB.r;
import static org.junit.Assert.assertEquals;

public class InsertData extends Base {

    @Test
    public void insert_data_to_users() {
        db.recreateTable(S.users, S.userId);
        db.createIndex(S.users, S.internalId);

        db.bulkInsert(S.users, C.userCount, iUser -> {

            String userId = Format.userId(iUser);

            HashMap<String, Object> rec = new HashMap<>();
            rec.put(S.userId, userId);
            rec.put("firstname", userId + "名");
            rec.put("nickName", userId + "ニックネーム");
            rec.put("surname", userId + "姓");
            rec.put(S.internalId, userId + S.internalId);
            rec.put(S.createdAt, OffsetDateTime.now());

            return rec;
        });
    }

    @Test
    public void insert_data_to_tours() {
        db.recreateTable(S.tours, S.tourId);
        db.createIndex(S.tours, S.userId);
        db.createIndex(S.tours, S.createdAt);

        db.bulkInsert(S.tours, C.tourCount, iTour -> {

            String tourId = Format.tourId(iTour);

            long iConducteur = iTour % C.conducteurCount;
            String conducteurId = Format.userId(iConducteur);

            HashMap<String, Object> rec = new HashMap<>();
            rec.put(S.tourId, tourId);
            rec.put(S.userId, conducteurId);
            rec.put("bagSize", "20Kg");
            rec.put("seatsTotal", 3);
            rec.put("seatsRemain", 3);
            rec.put(S.createdAt, OffsetDateTime.now());

            return rec;
        });
    }

    @Test
    public void insert_data_to_bookings() {
        db.recreateTable(S.bookings, S.bookingId);
        db.createIndex(S.bookings, S.userId);
        db.createIndex(S.bookings, S.tourId);
        db.createIndex(S.bookings, S.tourIdAndStatus, new String[]{S.tourId, S.status});
        db.createIndex(S.bookings, S.createdAt);

        db.recreateTable(S._selfBookings, S.bookingId);

        db.bulkInsert(S.bookings, C.bookingCount, iBooking -> {

            String bookingId = Format.bookingId(iBooking);

            long iTour = iBooking / C.bookingsPerTour;
            String tourId = Format.tourId(iTour);

            long iConducteur = iTour % C.conducteurCount;
            String conducteurId = Format.userId(iConducteur);

            long iPassenger = iBooking % C.passengerCount;
            String passengerId = Format.userId(C.userCount - iPassenger - 1);

            String bookingStatus = iBooking % C.bookingsPerTour < C.approvedBookingsPerTour ? S.approved : S.canceled;

            HashMap<String, Object> rec = new HashMap<>();
            rec.put(S.bookingId, bookingId);
            rec.put(S.tourId, tourId);
            rec.put(S.userId, passengerId);
            rec.put(S.status, bookingStatus);
            rec.put(S.createdAt, OffsetDateTime.now());

            if (conducteurId.equals(passengerId)) {
                db.bulkInsert(S._selfBookings, Arrays.asList(r.hashMap(S.bookingId, bookingId)));
            }

            return rec;
        });
    }

    HashMap<String, Object> newReview(long iReview, String tourId, String byUserId, String ofUserId, String bookingId) {
        String reviewId = Format.reviewId(iReview);

        HashMap<String, Object> rec = new HashMap<>();
        rec.put(S.reviewId, reviewId);
        rec.put(S.tourId, tourId);
        rec.put(S.ofUserId, ofUserId);
        rec.put(S.byUserId, byUserId);
        rec.put(S.comment, reviewId + " コメント from " + byUserId + " to " + ofUserId + " for " + tourId + " " + bookingId);
        rec.put(S.starRank, iReview % 4 + 1);
        rec.put(S.createdAt, OffsetDateTime.now());

        if (byUserId.equals(ofUserId)) {
            db.bulkInsert(S._selfReviews, Arrays.asList(rec));
        }

        return rec;
    }

    @Test
    public void insert_data_to_reviews() {
        db.recreateTable(S.reviews, S.reviewId);
        db.createIndex(S.reviews, S.tourId);
        db.createIndex(S.reviews, S.byUserId);
        db.createIndex(S.reviews, S.ofUserId);
        db.createIndex(S.reviews, S.createdAt);
        db.createIndex(S.reviews, S.tourIdAndbyUserIdAndofUserId, new String[]{S.tourId, S.byUserId, S.ofUserId});

        db.recreateTable(S._selfReviews, S.reviewId);

        db.bulkInsert(S.reviews, C.bookingCount, (iReview, iBooking) -> {

            boolean isApprovedBooking = iBooking % C.bookingsPerTour < C.approvedBookingsPerTour;
            if (!isApprovedBooking)
                return null; //skip

            long iTour = iBooking / C.bookingsPerTour;
            String tourId = Format.tourId(iTour);

            long iConducteur = iTour % C.conducteurCount;
            String conducteurId = Format.userId(iConducteur);

            long iPassenger = iBooking % C.passengerCount;
            String passengerId = Format.userId(C.userCount - iPassenger - 1);

            String bookingId = Format.bookingId(iBooking);

            return new Object[]{
                    newReview(iReview++, tourId, conducteurId, passengerId, bookingId)
                    , newReview(iReview++, tourId, passengerId, conducteurId, bookingId)
            };
        });
    }
}
