import com.rethinkdb.model.OptArgs;
import org.junit.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;

import static com.rethinkdb.RethinkDB.r;
import static org.junit.Assert.assertEquals;

public class CheckData extends Base {

    @Test
    public void check_users() {
        String minUserId = Format.userId(0);
        String maxUserId = Format.userId(C.userCount - 1);
        logger.info("except user count: {}, min: {} - max: {}", C.userCount, minUserId, maxUserId);

        assertEquals(C.userCount, db.getTableRowCount(S.users));
        assertEquals(minUserId, r.table(S.users).min().optArg(S.index, S.userId).g(S.userId).run(db.c));
        assertEquals(maxUserId, r.table(S.users).max().optArg(S.index, S.userId).g(S.userId).run(db.c));

        assertEquals(minUserId + S.internalId, r.table(S.users).min().optArg(S.index, S.internalId).g(S.internalId).run(db.c));
        assertEquals(maxUserId + S.internalId, r.table(S.users).max().optArg(S.index, S.internalId).g(S.internalId).run(db.c));
    }

    @Test
    public void check_tours() {

        String minTourId = Format.tourId(0);
        String maxTourId = Format.tourId(C.tourCount - 1);
        logger.info("except tour count: {}, min: {}, max: {}", C.tourCount, minTourId, maxTourId);

        assertEquals(C.tourCount, db.getTableRowCount(S.tours));
        assertEquals(minTourId, r.table(S.tours).min().optArg(S.index, S.tourId).g(S.tourId).run(db.c));
        assertEquals(maxTourId, r.table(S.tours).max().optArg(S.index, S.tourId).g(S.tourId).run(db.c));

        assertEquals(C.conducteurCount,
                (long) r.table(S.tours).distinct().optArg(S.index, S.userId).count().run(db.c));
        assertEquals(Format.userId(0),
                r.table(S.tours).min().optArg(S.index, S.userId).g(S.userId).run(db.c));
        assertEquals(Format.userId(C.conducteurCount - 1),
                r.table(S.tours).max().optArg(S.index, S.userId).g(S.userId).run(db.c));

        //////////////////////////////////////////////////////////////////////
        //generic checks
        assertEquals("should no any `userId` which does not exist in `users`", 0,
                (long) r.table(S.tours).distinct().optArg(S.index, S.userId)
                        .filter(userId -> r.not(r.table(S.users).get(userId)))
                        .count()
                        .run(db.c));
    }

    @Test
    public void check_bookings() {
        long count, exceptedCount;

        String minBookingId = Format.bookingId(0);
        String maxBookingId = Format.bookingId(C.bookingCount - 1);
        logger.info("except booking count: {}, min: {}, max: {}", C.bookingCount, minBookingId, maxBookingId);

        assertEquals(C.bookingCount, db.getTableRowCount(S.bookings));
        assertEquals(minBookingId, r.table(S.bookings).min().optArg(S.index, S.bookingId).g(S.bookingId).run(db.c));
        assertEquals(maxBookingId, r.table(S.bookings).max().optArg(S.index, S.bookingId).g(S.bookingId).run(db.c));

        assertEquals(C.passengerCount,
                (long) r.table(S.bookings).distinct().optArg(S.index, S.userId).count().run(db.c));
        assertEquals(Format.userId(C.userCount - C.passengerCount),
                r.table(S.bookings).min().optArg(S.index, S.userId).g(S.userId).run(db.c));
        assertEquals(Format.userId(C.userCount - 1),
                r.table(S.bookings).max().optArg(S.index, S.userId).g(S.userId).run(db.c));

        assertEquals("should have specified numbers of unique `tourId`", C.bookingTourCount,
                (long) r.table(S.bookings).distinct().optArg(S.index, S.tourId).count().run(db.c));

        // r.db("rcs").table("bookings")
        // .group({index:"tourId"})
        // .count(r.row("status").eq("approved"))
        // .ungroup()
        // .filter(r.row("reduction").eq(3))

        count = r.table(S.bookings).group().optArg(S.index, S.tourId)
                .count(row -> row.g(S.status).eq(S.approved))
                .ungroup()
                .count(row -> row.g(S.reduction).eq(C.approvedBookingsPerTour))
                .run(db.c, OptArgs.of(S.array_limit, C.bookingCount));
        exceptedCount = C.bookingTourCount -
                (C.bookingCount % C.bookingsPerTour == 0
                        || C.bookingCount % C.bookingsPerTour >= C.approvedBookingsPerTour
                        ? 0 : 1);
        assertEquals("should almost every tour have " + C.approvedBookingsPerTour + " approved applicants",
                exceptedCount, count);

        count = r.table(S.bookings).group().optArg(S.index, S.tourId)
                .count(row -> row.g(S.status).ne(S.approved))
                .ungroup()
                .filter(row -> row.g(S.reduction).eq(C.bookingsPerTour - C.approvedBookingsPerTour))
                .count()
                .run(db.c, OptArgs.of(S.array_limit, C.bookingCount));
        exceptedCount = C.bookingTourCount - (C.bookingCount % C.bookingsPerTour == 0 ? 0 : 1);
        assertEquals("should almost every tour have some non-approved applicants",
                exceptedCount, count);

        count = r.table(S.bookings).group().optArg(S.index, S.tourId)
                .g(S.userId).distinct().count()
                .ungroup()
                .filter(row -> row.g(S.reduction).eq(C.bookingsPerTour))
                .count()
                .run(db.c, OptArgs.of(S.array_limit, C.bookingCount));
        exceptedCount = C.bookingTourCount - (C.bookingCount % C.bookingsPerTour == 0 ? 0 : 1);
        assertEquals("should almost every tour have " + C.bookingsPerTour + " unique applicants",
                exceptedCount, count);

        //////////////////////////////////////////////////////////////////////
        //generic checks
        assertEquals("should no any `userId` which does not exist in `users`", 0,
                (long) r.table(S.bookings).distinct().optArg(S.index, S.userId)
                        .filter(userId -> r.not(r.table(S.users).get(userId)))
                        .count()
                        .run(db.c));

        assertEquals("should no any `tourId` which does not exist in `tours`", 0,
                (long) r.table(S.bookings).distinct().optArg(S.index, S.tourId)
                        .filter(tourId -> r.not(r.table(S.tours).get(tourId)))
                        .count()
                        .run(db.c));
    }

    @Test
    public void check_reviews() {

        String minReviewId = Format.reviewId(0);
        String maxReviewId = Format.reviewId(C.reviewCount - 1);
        logger.info("except review count: {}, min: {}, max: {}", C.reviewCount, minReviewId, maxReviewId);

        assertEquals(C.reviewCount, db.getTableRowCount(S.reviews));
        assertEquals(minReviewId, r.table(S.reviews).min().optArg(S.index, S.reviewId).g(S.reviewId).run(db.c));
        assertEquals(maxReviewId, r.table(S.reviews).max().optArg(S.index, S.reviewId).g(S.reviewId).run(db.c));

        long reviewTourCount = C.bookingTourCount;
        assertEquals(reviewTourCount,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.tourId).count().run(db.c));
        assertEquals(Format.tourId(0),
                r.table(S.reviews).min().optArg(S.index, S.tourId).g(S.tourId).run(db.c));
        assertEquals(Format.tourId(reviewTourCount - 1),
                r.table(S.reviews).max().optArg(S.index, S.tourId).g(S.tourId).run(db.c));

        //////////////////////////////////////////////////////////////////////
        //generic checks
        assertEquals("should no any `ofUserId` which does not exist in `users`", 0,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.ofUserId)
                        .filter(userId -> r.not(r.table(S.users).get(userId)))
                        .count()
                        .run(db.c));

        assertEquals("should no any `byUserId` which does not exist in `users`", 0,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.byUserId)
                        .filter(userId -> r.not(r.table(S.users).get(userId)))
                        .count()
                        .run(db.c));

        assertEquals("should no any `tourId` which does not exist in `tours`", 0,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.tourId)
                        .filter(tourId -> r.not(r.table(S.tours).get(tourId)))
                        .count()
                        .run(db.c));

        assertEquals("should no any `tourId` which does not exist in `bookings`", 0,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.tourId)
                        .filter(tourId ->
                                r.table(S.bookings).getAll(tourId).optArg(S.index, S.tourId)
                                        .isEmpty()
                        )
                        .count()
                        .run(db.c));

        assertEquals("should no any `tourId` which booking are not approved", 0,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.tourId)
                        .filter(tourId ->
                                r.table(S.bookings).getAll(tourId).optArg(S.index, S.tourId)
                                        .filter(row -> row.g(S.status).eq(S.approved))
                                        .isEmpty()
                        )
                        .count()
                        .run(db.c));

        assertEquals("should no any `ofUserId` which does not exist in `bookings` or `tours`", 0,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.ofUserId)
                        .filter(userId ->
                                r.table(S.bookings).getAll(userId).optArg(S.index, S.userId)
                                        .isEmpty()
                                        .and(r.table(S.tours).getAll(userId).optArg(S.index, S.userId)
                                                .isEmpty()
                                        )
                        )
                        .count()
                        .run(db.c));

        assertEquals("should no any `byUserId` which does not exist in `bookings` or `tours`", 0,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.byUserId)
                        .filter(userId ->
                                r.table(S.bookings).getAll(userId).optArg(S.index, S.userId)
                                        .isEmpty()
                                        .and(r.table(S.tours).getAll(userId).optArg(S.index, S.userId)
                                                .isEmpty()
                                        )
                        )
                        .count()
                        .run(db.c));

        assertEquals("every review should have unique [`tourId`,`byUserId`,`ofUserId`]", C.reviewCount,
                (long) r.table(S.reviews).distinct().optArg(S.index, S.tourIdAndbyUserIdAndofUserId)
                        .count()
                        .run(db.c)
                        + db.getTableRowCount(S._selfReviews) / 2);

    }

    @Test
    public void testSelectByDateIndex() {
        HashMap<String, Object> map;
        long count;

        String reviewId = Format.tourId(C.tourCount / 2);
        OffsetDateTime dt = OffsetDateTime.of(1900, 12, 31, 23, 58, 59, 123, ZoneOffset.UTC);

        map = r.table(S.tours).get(reviewId).update(r.hashMap(S.createdAt, dt)).run(db.c);
        assertEquals(1L, ((Long) map.get(S.replaced)).longValue());

        count = r.table(S.tours).getAll(dt).optArg(S.index, S.createdAt).count().run(db.c);
        assertEquals(1, count);
    }
}
