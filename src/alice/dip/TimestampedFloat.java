/*************
 * cil
 **************/
/*
 *  Structure used to keep float  values that change in time (e.g. Beam Energy)
 *
 */
package alice.dip;

public record TimestampedFloat (long time, float value) {
}
