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

package alice.dip.beam.mode;


import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import alice.dip.adapters.BeamModeProtoAdapter;
import alice.dip.AliDip2BK;
import alice.dip.enums.BeamModeEnum;
import alice.dip.LhcInfoObj;
import alice.dip.kafka.KafkaProducerInterface;

import ch.cern.alice.o2.control.common.Common;
import ch.cern.alice.o2.control.events.Events;

/**
 * Kafka producer for LHC Beam Mode events, serialized using Protocol Buffers.
 */
public class BeamModeEventsKafkaProducer extends KafkaProducerInterface<Integer, byte[]> {
	public static final String KAFKA_PRODUCER_TOPIC_DIP = "dip.lhc.beam_mode";

  /**
   * Constructor to create a BeamModeEventsKafkaProducer
   * @param bootstrapServers - Kafka bootstrap servers connection string in format of host:port
   */
  public BeamModeEventsKafkaProducer(String bootstrapServers) {
      super(bootstrapServers, KAFKA_PRODUCER_TOPIC_DIP, new IntegerSerializer(), new ByteArraySerializer());
      AliDip2BK.log(2, "BeamModeEventsKafkaProducer", "Initialized producer for topic: " + KAFKA_PRODUCER_TOPIC_DIP);
  }

  /**
   * Given a fill number for partitioning, a LhcInfoObj containing fill information,
   * and a timestamp, creates and sends a proto serialized Beam Mode Event to the Kafka topic.
   * @param fillNumber - fill number to be used for partition to ensure ordering
   * @param fill - LhcInfoObj containing fill information
   * @param timestamp - event timestamp at which the beam mode change event was received from DIP
   */
  public void sendEvent(Integer fillNumber, LhcInfoObj fill, long timestamp) {
      String beamModeStr = fill.getBeamMode();
      BeamModeEnum beamMode = BeamModeProtoAdapter.fromStringToEnum(beamModeStr);

      Common.BeamInfo beamInfo = Common.BeamInfo.newBuilder()
          .setStableBeamsStart(fill.getStableBeamStart())
          .setStableBeamsEnd(fill.getStableBeamStop())
          .setFillNumber(fill.fillNo)
          .setFillingSchemeName(fill.LHCFillingSchemeName)
          .setBeamMode(Common.BeamMode.valueOf(beamMode.name()))
          .setBeamType(fill.beamType)
          .build();

      Events.Ev_BeamModeEvent beamModeEvent = Events.Ev_BeamModeEvent.newBuilder()
          .setTimestamp(timestamp)
          .setBeamInfo(beamInfo)
          .build();

      Events.Event event = Events.Event.newBuilder()
          .setTimestamp(timestamp)
          .setTimestampNano((timestamp) * 1000000)
          .setBeamModeEvent(beamModeEvent)
          .build();
      byte[] value = event.toByteArray();

      send(fillNumber, value);
      AliDip2BK.log(2, "BeamModeEventsKafkaProducer", "Sent Beam Mode event for fill " + fill.fillNo + " with mode " + fill.getBeamMode() + " at timestamp " + timestamp);
  }
}