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

package alice.dip.enums;

/**
 * Java enum matching the BeamMode values from DIP service and common.proto
 * @enum BeamModeEnum
 */
public enum BeamModeEnum {
  UNKNOWN("UNKNOWN"),
  SETUP("SETUP"),
  ABORT("ABORT"),
  INJECTION_PROBE_BEAM("INJECTION PROBE BEAM"),
  INJECTION_SETUP_BEAM("INJECTION SETUP BEAM"),
  INJECTION_PHYSICS_BEAM("INJECTION PHYSICS BEAM"),
  PREPARE_RAMP("PREPARE RAMP"),
  RAMP("RAMP"),
  FLAT_TOP("FLAT TOP"),
  SQUEEZE("SQUEEZE"),
  ADJUST("ADJUST"),
  STABLE_BEAMS("STABLE BEAMS"),
  LOST_BEAMS("LOST BEAMS"),
  UNSTABLE_BEAMS("UNSTABLE BEAMS"),
  BEAM_DUMP_WARNING("BEAM DUMP WARNING"),
  BEAM_DUMP("BEAM DUMP"),
  RAMP_DOWN("RAMP DOWN"),
  CYCLING("CYCLING"),
  RECOVERY("RECOVERY"),
  INJECT_AND_DUMP("INJECT AND DUMP"),
  CIRCULATE_AND_DUMP("CIRCULATE AND DUMP"),
  NO_BEAM("NO BEAM");

  public final String label;

  private BeamModeEnum(String label) {
      this.label = label;
  }
}
