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

package alice.dip.kafka;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import alice.dip.AliDip2BK;
import alice.dip.LhcInfoObj;
import alice.dip.kafka.events.Events;
import alice.dip.kafka.events.Common;

/**
 * Kafka producer for LHC Beam Mode events, serialized using Protocol Buffers.
 */
public class BeamModeEventsKafkaProducer extends KafkaProducerInterface<Integer, byte[]> {
	public static String KAFKA_PRODUCER_TOPIC_DIP = "dip.lhc.beam_mode";

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
        Common.BeamInfo beamInfo = Common.BeamInfo.newBuilder()
            .setStableBeamsStart(fill.getStableBeamStart())
            .setStableBeamsEnd(fill.getStableBeamStop())
            .setFillNumber(fill.fillNo)
            .setFillingSchemeName(fill.LHCFillingSchemeName)
            .setBeamMode(Common.BeamMode.valueOf(fill.getBeamMode()))
            .setBeamType(fill.beamType)
            .build();

        Events.Ev_BeamModeEvent event = Events.Ev_BeamModeEvent.newBuilder()
            .setTimestamp(timestamp)
            .setBeamInfo(beamInfo)
            .build();
        byte[] value = event.toByteArray();

        send(fillNumber, value);
        AliDip2BK.log(2, "BeamModeEventsKafkaProducer", "Sent Beam Mode event for fill " + fill.fillNo + " with mode " + fill.getBeamMode() + " at timestamp " + timestamp);
    }
}
