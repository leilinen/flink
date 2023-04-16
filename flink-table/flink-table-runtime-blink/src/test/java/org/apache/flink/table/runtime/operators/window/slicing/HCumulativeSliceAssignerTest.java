package org.apache.flink.table.runtime.operators.window.slicing;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SliceAssigners.HCumulativeSliceAssigner}. */
@RunWith(Parameterized.class)
public class HCumulativeSliceAssignerTest extends SliceAssignerTestBase {

    @Parameterized.Parameter public ZoneId shiftTimeZone;

    @Parameterized.Parameters(name = "timezone = {0}")
    public static Collection<ZoneId> parameters() {
        return Arrays.asList(ZoneId.of("America/Los_Angeles"), ZoneId.of("Asia/Shanghai"));
    }

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner =
                SliceAssigners.hcumulative(
                        0, shiftTimeZone, Duration.ofDays(1), Duration.ofHours(6), Duration.ofHours(1));

        assertEquals(
                utcMills("1970-01-01T01:00:00"),
                assignSliceEnd(assigner, localMills("1970-01-01T00:00:00")));
        assertEquals(
                utcMills("1970-01-02T23:00:00"),
                assignSliceEnd(assigner, localMills("1970-01-02T22:59:59.999")));
        assertEquals(
                utcMills("1970-01-03T00:00:00"),
                assignSliceEnd(assigner, localMills("1970-01-02T23:00:00")));
    }



    @Test
    public void testDstSaving() {
        if (!TimeZone.getTimeZone(shiftTimeZone).useDaylightTime()) {
            return;
        }
        SliceAssigner assigner =
                SliceAssigners.hcumulative(
                        0, shiftTimeZone, Duration.ofDays(1), Duration.ofHours(6), Duration.ofHours(1));

        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele start at time 2021-03-14 02:00:00
        long epoch01 = 1615708800000L; // 2021-03-14 00:00:00
        long epoch02 = 1615712400000L; // 2021-03-14 01:00:00
        long epoch03 = 1615716000000L; // 2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
        long epoch04 = 1615719600000L; // 2021-03-14 04:00:00
        long epoch05 = 1615723200000L; // 2021-03-14 05:00:00
        long epoch06 = 1615726800000L; // 2021-03-14 06:00:00

        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T01:00", epoch01, assigner);
        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T02:00", epoch02, assigner);
        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T04:00", epoch03, assigner);
        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T05:00", epoch04, assigner);
        assertSliceStartEnd("2021-03-14T01:00", "2021-03-14T06:00", epoch05, assigner);
        assertSliceStartEnd("2021-03-14T02:00", "2021-03-14T07:00", epoch06, assigner);

        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele end at time 2021-11-07 02:00:00
        long epoch5 = 1636268400000L; // 2021-11-07 00:00:00
        long epoch6 = 1636272000000L; // the first local timestamp 2021-11-07 01:00:00
        long epoch7 = 1636275600000L; // rollback to  2021-11-07 01:00:00
        long epoch8 = 1636279200000L; // 2021-11-07 02:00:00
        long epoch9 = 1636282800000L; // 2021-11-07 03:00:00
        long epoch10 = 1636286400000L; // 2021-11-07 04:00:00

        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T01:00", epoch5, assigner);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T02:00", epoch6, assigner);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T02:00", epoch7, assigner);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T03:00", epoch8, assigner);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T04:00", epoch9, assigner);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T05:00", epoch10, assigner);
    }

    @Test
    public void testGetWindowStart() {
        SliceAssigner assigner =
                SliceAssigners.hcumulative(
                        0, shiftTimeZone, Duration.ofDays(1), Duration.ofHours(12), Duration.ofHours(1));
        assertEquals(
                utcMills("1969-12-31T12:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T00:00:00")));
        assertEquals(
                utcMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T01:00:00")));
        assertEquals(
                utcMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T02:00:00")));
        assertEquals(
                utcMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T03:00:00")));
        assertEquals(
                utcMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T04:00:00")));
        assertEquals(
                utcMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T05:00:00")));
        assertEquals(
                utcMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T06:00:00")));
        assertEquals(
                utcMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T08:00:00")));
        assertEquals(
                utcMills("1970-01-01T12:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T13:00:00")));
        assertEquals(
                utcMills("1970-01-01T12:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T14:00:00")));
        assertEquals(
                utcMills("1970-01-01T12:00:00"),
                assigner.getWindowStart(utcMills("1970-01-01T15:00:00")));
    }

    @Test
    public void testExpiredSlices() {
        SliceAssigner assigner =
                SliceAssigners.hcumulative(
                        0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1), Duration.ofMinutes(1));

        // reuse the first slice, skip to cleanup it
        assertEquals(
                Collections.emptyList(), expiredSlices(assigner, utcMills("1970-01-01T01:00:00")));

        assertEquals(
                Collections.singletonList(utcMills("1970-01-01T02:00:00")),
                expiredSlices(assigner, utcMills("1970-01-01T02:00:00")));
        assertEquals(
                Collections.singletonList(utcMills("1970-01-01T03:00:00")),
                expiredSlices(assigner, utcMills("1970-01-01T03:00:00")));
        assertEquals(
                Collections.singletonList(utcMills("1970-01-01T04:00:00")),
                expiredSlices(assigner, utcMills("1970-01-01T04:00:00")));
        assertEquals(
                Arrays.asList(utcMills("1970-01-01T05:00:00"), utcMills("1970-01-01T01:00:00")),
                expiredSlices(assigner, utcMills("1970-01-01T05:00:00")));

        // reuse the first slice, skip to cleanup it
        assertEquals(
                Collections.emptyList(), expiredSlices(assigner, utcMills("1970-01-01T06:00:00")));

        assertEquals(
                Arrays.asList(utcMills("1970-01-01T10:00:00"), utcMills("1970-01-01T06:00:00")),
                expiredSlices(assigner, utcMills("1970-01-01T10:00:00")));
        assertEquals(
                Arrays.asList(utcMills("1970-01-01T00:00:00"), utcMills("1969-12-31T20:00:00")),
                expiredSlices(assigner, utcMills("1970-01-01T00:00:00")));
    }



    private long localMills(String timestampStr) {
        return localMills(timestampStr, shiftTimeZone);
    }



}
