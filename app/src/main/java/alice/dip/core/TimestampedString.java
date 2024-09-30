/*************
 * cil
 **************/

/*
 *  Structure used to keep String values that change in time (e.g. BeamMode)
 *
 */
package alice.dip.core;

public record TimestampedString (long time, String value) {
}
