package org.apache.flink.table.planner.plan.logical;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

/** Logical representation of a hop cumulative window specification. */
@JsonTypeName("HCumulativeWindow")
public class HCumulativeWindowSpec implements WindowSpec {

    public static final String FIELD_NAME_MAX_SIZE = "maxSize";
    public static final String FIELD_NAME_SLIDE = "slide";
    public static final String FIELD_NAME_STEP = "step";

    @JsonProperty(FIELD_NAME_MAX_SIZE)
    private final Duration maxSize;

    @JsonProperty(FIELD_NAME_SLIDE)
    private final Duration slide;

    @JsonProperty(FIELD_NAME_STEP)
    private final Duration step;

    @JsonCreator
    public HCumulativeWindowSpec(
            @JsonProperty(FIELD_NAME_MAX_SIZE) Duration maxSize,
            @JsonProperty(FIELD_NAME_SLIDE) Duration slide,
            @JsonProperty(FIELD_NAME_STEP) Duration step) {
        this.maxSize = checkNotNull(maxSize);
        this.slide = checkNotNull(slide);
        this.step = checkNotNull(step);
    }

    @Override
    public String toSummaryString(String windowing) {
        return String.format(
                "HCUMULATE(%s, max_size=[%s], slide=[%s])",
                windowing, formatWithHighestUnit(maxSize), formatWithHighestUnit(slide));
    }

    public Duration getMaxSize() {
        return maxSize;
    }

    public Duration getSlide() {
        return slide;
    }

    public Duration getStep() {
        return step;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HCumulativeWindowSpec that = (HCumulativeWindowSpec) o;
        return maxSize.equals(that.maxSize) && slide.equals(that.slide);
    }

    @Override
    public int hashCode() {
        return Objects.hash(HCumulativeWindowSpec.class, maxSize, slide);
    }

    @Override
    public String toString() {
        return String.format(
                "HCUMULATE(max_size=[%s], slide=[%s])",
                formatWithHighestUnit(maxSize), formatWithHighestUnit(slide));
    }
}
