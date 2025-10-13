/**
 * @license
 * Copyright CERN and copyright holders of ALICE O2. This software is
 * distributed under the terms of the GNU General Public License v3 (GPL
 * Version 3), copied verbatim in the file "COPYING".
 *
 * See http://alice-o2.web.cern.ch/license for full licensing information.
 *
 * In applying this license CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as an Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */

package alice.dip.adapters;

import alice.dip.enums.BeamModeEnum;

/**
 * Adapter class to convert between string representations of beam modes and the BeamModeEnum.
 */
public class BeamModeProtoAdapter {

  /**
   * Returns the enum constant matching the given string, or UNKNOWN if not found.
   * Accepts both space and underscore separated names, case-insensitive.
   * @param beamMode The beam mode string to convert.
   * @return The corresponding BeamModeEnum constant, or UNKNOWN if not recognized.
   */
  public static BeamModeEnum fromStringToEnum(String beamMode) {
    if (beamMode == null || beamMode.trim().isEmpty()) {
      return BeamModeEnum.UNKNOWN;
    }
    for (BeamModeEnum value : BeamModeEnum.values()) {
      if (value.label.equalsIgnoreCase(beamMode)) {
        return value;
      }
    }
    return BeamModeEnum.UNKNOWN;
  }
}
