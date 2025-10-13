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

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import alice.dip.enums.BeamModeEnum;

/**
 * Unit tests for the BeamModeProtoAdapter class.
 */
class BeamModeProtoAdapterTest {
    
  @Test
  void shouldReturnUnknownToEmptyStrings() {
      assertEquals(BeamModeEnum.UNKNOWN, BeamModeProtoAdapter.fromStringToEnum(""));
      assertEquals(BeamModeEnum.UNKNOWN, BeamModeProtoAdapter.fromStringToEnum("   "));
  }

  @Test
  void shouldReturnBeamModeUnknownToNull() {
    assertEquals(BeamModeEnum.UNKNOWN, BeamModeProtoAdapter.fromStringToEnum(null));
  }
  
  @Test
  void shouldReturnBeamModeUnknownToInvalidStrings() {
    assertEquals(BeamModeEnum.UNKNOWN, BeamModeProtoAdapter.fromStringToEnum("INVALID"));
    assertEquals(BeamModeEnum.UNKNOWN, BeamModeProtoAdapter.fromStringToEnum("SETUP_BEAM"));
    assertEquals(BeamModeEnum.UNKNOWN, BeamModeProtoAdapter.fromStringToEnum("injection physics beam extra"));
  }

  @Test
  void shouldReturnCorrectBeamModeEnumForValidStrings() {
    assertEquals(BeamModeEnum.NO_BEAM, BeamModeProtoAdapter.fromStringToEnum("NO BEAM"));
    assertEquals(BeamModeEnum.INJECTION_PHYSICS_BEAM, BeamModeProtoAdapter.fromStringToEnum("INJECTION PHYSICS BEAM"));
    assertEquals(BeamModeEnum.INJECTION_PHYSICS_BEAM, BeamModeProtoAdapter.fromStringToEnum("injection physics beam"));
    assertEquals(BeamModeEnum.LOST_BEAMS, BeamModeProtoAdapter.fromStringToEnum("LOST BEAMS"));

    for (BeamModeEnum mode : BeamModeEnum.values()) {
      assertEquals(mode, BeamModeProtoAdapter.fromStringToEnum(mode.label));
      assertEquals(mode, BeamModeProtoAdapter.fromStringToEnum(mode.label.toLowerCase()));
    }
  }
}
