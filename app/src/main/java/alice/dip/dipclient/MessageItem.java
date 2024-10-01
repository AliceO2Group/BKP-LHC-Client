/*************
 * cil
 **************/

package alice.dip.dipclient;

import cern.dip.DipData;

public record MessageItem(String parameterName, DipData dipData) {
}
